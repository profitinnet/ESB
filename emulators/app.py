"""Pilot emulators for Creatio Account, SAP B1 Service Layer, and Directum RX.

SERVICE selects which contract this process speaks:
  creatio  — save an Account and call the bus, as a Creatio save-process would
  sap      — POST /b1s/v1/Login and BusinessPartners
  directum — POST/PATCH /Integration/odata/ICompanies
"""

import json
import os
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SERVICE = os.environ.get("SERVICE", "creatio")
BUS_URL = os.environ.get("BUS_URL", "http://ibus-camel:8080/ingress/accounts")
PORT = int(os.environ.get("PORT", "8080"))

lock = threading.Lock()
accounts = {}
partners = {}
sessions = set()
companies = {}
inbox = []
reject_codes = set()
directum_down = False
directum_data_error = False
directum_delay_s = 0
directum_unauthorized = False
directum_drop_create = False
next_company_id = 1


def now_iso():
    # Milliseconds stay in the timestamp. Two saves in the same second are two versions.
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def read_body(handler):
    length = handler.headers.get("Content-Length")
    if length not in (None, ""):
        return handler.rfile.read(int(length))
    if "chunked" in handler.headers.get("Transfer-Encoding", "").lower():
        chunks = []
        while True:
            line = handler.rfile.readline()
            if not line:
                break
            size = int(line.split(b";", 1)[0].strip() or b"0", 16)
            if size == 0:
                handler.rfile.readline()
                break
            chunks.append(handler.rfile.read(size))
            handler.rfile.read(2)
        return b"".join(chunks)
    return b""


def read_json(handler):
    raw = read_body(handler)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def send(handler, status, payload=None, headers=None):
    body = b""
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    for key, value in (headers or {}).items():
        handler.send_header(key, value)
    handler.end_headers()
    if body and handler.command != "HEAD":
        handler.wfile.write(body)


def path_of(handler):
    return handler.path.split("?", 1)[0]


def query_of(handler):
    parsed = urllib.parse.urlparse(handler.path)
    return {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()}


def cookie_session(handler):
    cookie = handler.headers.get("Cookie", "")
    for part in cookie.split(";"):
        name, _, value = part.strip().partition("=")
        if name == "B1SESSION" and value:
            return value
    return ""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("%s %s" % (self.address_string(), fmt % args), flush=True)

    def do_GET(self):
        if SERVICE == "sap":
            self.sap_get()
        elif SERVICE == "directum":
            self.directum_get()
        elif SERVICE == "creatio":
            self.creatio_get()
        else:
            send(self, 404, {"error": "unknown service"})

    def do_POST(self):
        path = path_of(self)
        if path == "/clear":
            self.clear_all()
            return
        if SERVICE == "sap":
            self.sap_post()
        elif SERVICE == "directum":
            self.directum_post()
        elif SERVICE == "creatio":
            self.creatio_save()
        else:
            send(self, 404, {"error": "unknown service"})

    def clear_all(self):
        with lock:
            accounts.clear()
            partners.clear()
            sessions.clear()
            companies.clear()
            inbox.clear()
            reject_codes.clear()
            global next_company_id, directum_down, directum_data_error, directum_delay_s
            global directum_unauthorized, directum_drop_create
            directum_down = False
            directum_data_error = False
            directum_delay_s = 0
            directum_unauthorized = False
            directum_drop_create = False
            next_company_id = 1
        send(self, 200, {"status": "cleared"})

    def do_PATCH(self):
        if SERVICE == "sap":
            self.sap_patch()
        elif SERVICE == "directum":
            self.directum_patch()
        else:
            send(self, 405, {"error": "method not allowed"})

    def do_PUT(self):
        if SERVICE == "creatio":
            self.creatio_save()
        else:
            send(self, 405, {"error": "method not allowed"})

    def creatio_get(self):
        path = path_of(self)
        if path == "/accounts":
            with lock:
                send(self, 200, {"value": list(accounts.values())})
            return
        if path.startswith("/accounts/"):
            account_id = urllib.parse.unquote(path[len("/accounts/"):])
            with lock:
                account = accounts.get(account_id)
            if not account:
                send(self, 404, {"error": "account not found"})
                return
            send(self, 200, account)
            return
        send(self, 404, {"error": "not found"})

    def creatio_save(self):
        try:
            body = read_json(self)
        except json.JSONDecodeError:
            send(self, 400, {"error": "invalid json"})
            return
        path = path_of(self)
        account_id = body.get("Id") or ""
        if path.startswith("/accounts/"):
            account_id = urllib.parse.unquote(path[len("/accounts/"):])
        if not body.get("Name"):
            send(self, 400, {"error": "Name is required"})
            return
        if not account_id:
            account_id = str(uuid.uuid4())
        account = {
            "Id": account_id,
            "Name": body.get("Name"),
            "Code": body.get("Code") or "",
            "Phone": body.get("Phone") or "",
            "Web": body.get("Web") or "",
            "Address": body.get("Address") or "",
            "Zip": body.get("Zip") or "",
            "Notes": body.get("Notes") or "",
            "AlternativeName": body.get("AlternativeName") or "",
            "ModifiedOn": now_iso(),
            "TaxId": body.get("TaxId") or "",
        }
        with lock:
            accounts[account_id] = account
        if query_of(self).get("publish", "1") != "0":
            try:
                call_bus(account)
            except Exception as exc:
                send(self, 502, {"error": "bus call failed", "detail": str(exc), "account": account})
                return
        send(self, 200, account)

    def sap_authorized(self):
        session = cookie_session(self)
        if session and session in sessions:
            return True
        send(self, 401, {"error": {"code": 401, "message": {"lang": "en-us", "value": "Invalid session"}}})
        return False

    def sap_get(self):
        path = path_of(self)
        if path == "/inbox":
            with lock:
                send(self, 200, {"value": list(inbox)})
            return
        if path in ("/b1s/v1/BusinessPartners", "/b1s/v2/BusinessPartners"):
            if not self.sap_authorized():
                return
            with lock:
                send(self, 200, {"value": list(partners.values())})
            return
        card = card_code_from_path(path)
        if card is None:
            send(self, 404, {"error": "not found"})
            return
        if not self.sap_authorized():
            return
        with lock:
            partner = partners.get(card)
        if not partner:
            send(self, 404, {"error": {"code": -2028, "message": {"lang": "en-us", "value": "No matching records found"}}})
            return
        send(self, 200, partner)

    def sap_post(self):
        path = path_of(self)
        try:
            body = read_json(self)
        except json.JSONDecodeError:
            send(self, 400, {"error": {"code": -1, "message": {"lang": "en-us", "value": "invalid json"}}})
            return
        if path in ("/b1s/v1/Login", "/b1s/v2/Login"):
            if not body.get("UserName") or not body.get("CompanyDB"):
                send(self, 401, {"error": {"code": 401, "message": {"lang": "en-us", "value": "Login failed"}}})
                return
            session = uuid.uuid4().hex
            with lock:
                sessions.add(session)
            send(self, 200, {"SessionId": session, "Version": "10.00"}, {"Set-Cookie": "B1SESSION=%s; Path=/" % session})
            return
        if path not in ("/b1s/v1/BusinessPartners", "/b1s/v2/BusinessPartners"):
            send(self, 404, {"error": "not found"})
            return
        if not self.sap_authorized():
            return
        remember("POST", path, body)
        card = body.get("CardCode") or ""
        name = body.get("CardName") or ""
        if not name:
            send(self, 400, {"error": {"code": -5002, "message": {"lang": "en-us", "value": "CardName is required"}}})
            return
        if not card:
            send(self, 400, {"error": {"code": -5002, "message": {"lang": "en-us", "value": "CardCode is required"}}})
            return
        with lock:
            if card in partners:
                send(self, 400, {"error": {"code": -10, "message": {"lang": "en-us", "value": "Business partner code '%s' already assigned to a business partner" % card}}})
                return
            partner = normalize_partner(body, card)
            partners[card] = partner
        send(self, 201, partner)

    def sap_patch(self):
        card = card_code_from_path(path_of(self))
        if card is None:
            send(self, 404, {"error": "not found"})
            return
        if not self.sap_authorized():
            return
        try:
            body = read_json(self)
        except json.JSONDecodeError:
            send(self, 400, {"error": {"code": -1, "message": {"lang": "en-us", "value": "invalid json"}}})
            return
        remember("PATCH", path_of(self), body)
        with lock:
            partner = partners.get(card)
            if not partner:
                send(self, 404, {"error": {"code": -2028, "message": {"lang": "en-us", "value": "No matching records found"}}})
                return
            if "CardName" in body and not body.get("CardName"):
                send(self, 400, {"error": {"code": -5002, "message": {"lang": "en-us", "value": "CardName is required"}}})
                return
            for field in ("CardName", "Phone1", "Website", "FederalTaxID", "CardType"):
                if field in body:
                    partner[field] = body[field]
            if "BPAddresses" in body:
                partner["BPAddresses"] = body["BPAddresses"]
        send(self, 204, None)

    def directum_authorized(self):
        if self.headers.get("Username") and self.headers.get("Password"):
            return True
        send(self, 401, {"error": {"message": "Username and Password headers are required"}})
        return False

    def directum_get(self):
        path = path_of(self)
        if path == "/inbox":
            with lock:
                send(self, 200, {"value": list(inbox)})
            return
        if directum_is_down():
            send(self, 503, {"error": {"message": "Directum unavailable"}})
            return
        if directum_unauthorized_on():
            send(self, 401, {"error": {"message": "unauthorized"}})
            return
        if path != "/Integration/odata/ICompanies":
            send(self, 404, {"error": "not found"})
            return
        if not self.directum_authorized():
            return
        code = query_of(self).get("code")
        with lock:
            rows = list(companies.values())
        if code is not None:
            rows = [row for row in rows if row.get("Code") == code]
        send(self, 200, {"value": rows})

    def directum_post(self):
        path = path_of(self)
        if path in ("/pilot/down", "/pilot/up"):
            global directum_down
            with lock:
                directum_down = path == "/pilot/down"
            send(self, 200, {"status": "down" if directum_down else "up"})
            return
        if path in ("/pilot/data-error", "/pilot/data-ok"):
            global directum_data_error
            with lock:
                directum_data_error = path == "/pilot/data-error"
            send(self, 200, {"status": "data-error" if directum_data_error else "data-ok"})
            return
        if path in ("/pilot/unauthorized", "/pilot/authorized"):
            global directum_unauthorized
            with lock:
                directum_unauthorized = path == "/pilot/unauthorized"
            send(self, 200, {"status": "unauthorized" if directum_unauthorized else "authorized"})
            return
        if path == "/pilot/drop-create":
            global directum_drop_create
            with lock:
                directum_drop_create = True
            send(self, 200, {"status": "drop-create"})
            return
        if path == "/pilot/delay":
            try:
                body = read_json(self)
            except json.JSONDecodeError:
                send(self, 400, {"error": {"message": "invalid json"}})
                return
            global directum_delay_s
            with lock:
                directum_delay_s = float(body.get("seconds") or 0)
            send(self, 200, {"status": "delay", "seconds": directum_delay_s})
            return
        if path in ("/pilot/reject", "/pilot/release"):
            try:
                body = read_json(self)
            except json.JSONDecodeError:
                send(self, 400, {"error": {"message": "invalid json"}})
                return
            code = body.get("code") or ""
            with lock:
                if path == "/pilot/reject" and code:
                    reject_codes.add(code)
                else:
                    reject_codes.discard(code)
            send(self, 200, {"status": "rejecting" if path == "/pilot/reject" else "released", "code": code})
            return
        if path != "/Integration/odata/ICompanies":
            send(self, 404, {"error": "not found"})
            return
        if not self.directum_authorized():
            return
        try:
            body = read_json(self)
        except json.JSONDecodeError:
            send(self, 400, {"error": {"message": "invalid json"}})
            return
        remember("POST", path_of(self), body)
        if directum_is_down():
            send(self, 503, {"error": {"message": "Directum unavailable"}})
            return
        if directum_unauthorized_on():
            send(self, 401, {"error": {"message": "unauthorized"}})
            return
        if directum_data_error_on():
            send(self, 400, {"error": {"message": "data rejected"}})
            return
        if directum_must_reject(self, body, body.get("Code") or ""):
            send(self, 400, {"error": {"message": "forced failure for pilot check"}})
            return
        if not body.get("Name"):
            send(self, 400, {"error": {"message": "Name is required"}})
            return
        global next_company_id
        with lock:
            existing = next((row for row in companies.values() if body.get("Code") and row.get("Code") == body.get("Code")), None)
            if existing:
                send(self, 409, {"error": {"message": "company with this Code already exists", "Id": existing["Id"]}})
                return
            company = {
                "Id": next_company_id,
                "Name": body.get("Name"),
                "TIN": body.get("TIN") or "",
                "Phone": body.get("Phone") or "",
                "LegalAddress": body.get("LegalAddress") or "",
                "Code": body.get("Code") or "",
                "Status": "Active",
            }
            next_company_id += 1
            companies[company["Id"]] = company
            drop = directum_drop_create
            if drop:
                directum_drop_create = False
        if drop:
            drop_response(self)
            return
        pilot_delay()
        send(self, 201, company)

    def directum_patch(self):
        if not self.directum_authorized():
            return
        company_id = company_id_from_path(path_of(self))
        if company_id is None:
            send(self, 404, {"error": "not found"})
            return
        try:
            body = read_json(self)
        except json.JSONDecodeError:
            send(self, 400, {"error": {"message": "invalid json"}})
            return
        remember("PATCH", path_of(self), body)
        if directum_is_down():
            send(self, 503, {"error": {"message": "Directum unavailable"}})
            return
        if directum_unauthorized_on():
            send(self, 401, {"error": {"message": "unauthorized"}})
            return
        if directum_data_error_on():
            send(self, 400, {"error": {"message": "data rejected"}})
            return
        with lock:
            current = companies.get(company_id)
            current_code = current.get("Code") if current else ""
        if directum_must_reject(self, body, current_code):
            send(self, 400, {"error": {"message": "forced failure for pilot check"}})
            return
        with lock:
            company = companies.get(company_id)
            if not company:
                send(self, 404, {"error": {"message": "company not found"}})
                return
            if "Name" in body and not body.get("Name"):
                send(self, 400, {"error": {"message": "Name is required"}})
                return
            for field in ("Name", "TIN", "Phone", "LegalAddress"):
                if field in body:
                    company[field] = body[field]
        pilot_delay()
        send(self, 200, company)


def drop_response(handler):
    handler.close_connection = True
    try:
        handler.connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def directum_unauthorized_on():
    with lock:
        return directum_unauthorized


def directum_is_down():
    with lock:
        return directum_down


def directum_data_error_on():
    with lock:
        return directum_data_error


def pilot_delay():
    with lock:
        seconds = directum_delay_s
    if seconds and seconds > 0:
        time.sleep(seconds)


def directum_must_reject(self, body, code):
    if body.get("Reject") is True or body.get("Name") == "FAIL-DIRECTUM":
        return True
    if self.headers.get("X-Pilot-Reject") == "directum":
        return True
    with lock:
        return bool(code) and code in reject_codes


def remember(method, path, body):
    with lock:
        inbox.append({"time": now_iso(), "method": method, "path": path, "body": body})
        del inbox[:-40]


def call_bus(account):
    data = json.dumps(account, ensure_ascii=False).encode("utf-8")
    delay = 0.5
    last = None
    for attempt in range(6):
        request = urllib.request.Request(
            BUS_URL,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                response.read()
                return
        except urllib.error.HTTPError as exc:
            if 400 <= exc.code < 500:
                raise
            last = exc
        except Exception as exc:
            last = exc
        if attempt < 5:
            time.sleep(delay)
            delay = min(delay * 2, 8)
    raise last


def card_code_from_path(path):
    marker = "/BusinessPartners("
    if marker not in path:
        return None
    inner = path.split(marker, 1)[1]
    inner = inner.split(")", 1)[0]
    if inner.startswith("CardCode="):
        inner = inner[len("CardCode="):]
    return urllib.parse.unquote(inner).strip("'\"")


def company_id_from_path(path):
    marker = "/ICompanies("
    if marker not in path:
        return None
    inner = path.split(marker, 1)[1].split(")", 1)[0].strip("'\"")
    try:
        return int(inner)
    except ValueError:
        return None


def normalize_partner(body, card):
    return {
        "CardCode": card,
        "CardName": body.get("CardName"),
        "CardType": body.get("CardType") or "cCustomer",
        "Phone1": body.get("Phone1") or "",
        "Website": body.get("Website") or "",
        "FederalTaxID": body.get("FederalTaxID") or "",
        "BPAddresses": body.get("BPAddresses") or [],
    }


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
