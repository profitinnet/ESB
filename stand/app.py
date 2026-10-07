"""Visual pilot stand.

The page is the only published address. Emulators stay on the server itself.
Topic reads assign partitions directly and do not join the SAP or Directum
reader groups, so looking at a topic does not move their position.
"""

import json
import os
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

PORT = int(os.environ.get("PORT", "8080"))
CREATIO_URL = os.environ.get("CREATIO_URL", "http://ibus-creatio:8080")
SAP_URL = os.environ.get("SAP_URL", "http://sap-b1:8080")
DIRECTUM_URL = os.environ.get("DIRECTUM_URL", "http://directum-rx:8080")
CAMEL_URL = os.environ.get("CAMEL_URL", "http://ibus-camel:8080")
BROKERS = os.environ.get("BROKERS", "redpanda:9092")
TOPICS = (
    "crm.account.upserted.v1",
    "dlq.sap.account",
    "dlq.directum.account",
    "replay.sap.account",
    "replay.directum.account",
)
PAGE = os.path.join(os.path.dirname(__file__), "index.html")
lock = threading.Lock()


def http_json(method, url, body=None, headers=None, timeout=8):
    data = None if body is None else json.dumps(body).encode("utf-8")
    hdrs = {}
    if body is not None:
        hdrs["Content-Type"] = "application/json"
    if headers:
        hdrs.update(headers)
    request = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            payload = json.loads(raw.decode("utf-8")) if raw else None
            return response.status, payload
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else None
        except json.JSONDecodeError:
            payload = raw.decode("utf-8", errors="replace") if raw else None
        return exc.code, payload


def inbox_rows(url):
    status, payload = http_json("GET", url + "/inbox")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError("inbox %s" % status)
    return payload.get("value") or []


def creatio_rows():
    status, payload = http_json("GET", CREATIO_URL + "/accounts")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError("Creatio %s" % status)
    return payload.get("value") or []


def sap_rows():
    status, login = http_json(
        "POST",
        SAP_URL + "/b1s/v1/Login",
        {"CompanyDB": "SBODEMO", "UserName": "manager", "Password": "manager"},
    )
    if status != 200 or not isinstance(login, dict) or not login.get("SessionId"):
        raise RuntimeError("SAP login %s" % status)
    status, payload = http_json(
        "GET",
        SAP_URL + "/b1s/v1/BusinessPartners",
        headers={"Cookie": "B1SESSION=" + login["SessionId"]},
    )
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError("SAP %s" % status)
    return payload.get("value") or []


def delivery_rows():
    status, payload = http_json("GET", CAMEL_URL + "/delivery-state")
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError("delivery_state %s" % status)
    return payload.get("value") or []


def directum_rows():
    status, payload = http_json(
        "GET",
        DIRECTUM_URL + "/Integration/odata/ICompanies",
        headers={"Username": "ibus", "Password": "ibus"},
    )
    if status != 200 or not isinstance(payload, dict):
        raise RuntimeError("Directum %s" % status)
    return payload.get("value") or []


def decode_message(raw):
    if raw is None:
        return None
    text = raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def read_topics(limit=8):
    consumer = KafkaConsumer(
        bootstrap_servers=[BROKERS],
        group_id=None,
        enable_auto_commit=False,
        consumer_timeout_ms=400,
        request_timeout_ms=8000,
        api_version_auto_timeout_ms=8000,
    )
    try:
        assigned = []
        for topic in TOPICS:
            parts = consumer.partitions_for_topic(topic) or set()
            assigned.extend(TopicPartition(topic, part) for part in sorted(parts))
        if not assigned:
            return {topic: {"count": 0, "messages": []} for topic in TOPICS}
        consumer.assign(assigned)
        ends = consumer.end_offsets(assigned)
        starts = consumer.beginning_offsets(assigned)
        counts = {topic: 0 for topic in TOPICS}
        targets = {}
        for tp in assigned:
            counts[tp.topic] += ends[tp] - starts[tp]
            begin = max(starts[tp], ends[tp] - limit)
            consumer.seek(tp, begin)
            targets[tp] = ends[tp]
        collected = {topic: [] for topic in TOPICS}
        idle = 0
        deadline = time.time() + 3
        while time.time() < deadline and idle < 2 and any(targets[tp] > consumer.position(tp) for tp in assigned):
            batch = consumer.poll(timeout_ms=250, max_records=200)
            if not batch:
                idle += 1
                continue
            idle = 0
            for tp, messages in batch.items():
                for message in messages:
                    collected[tp.topic].append(
                        {
                            "partition": message.partition,
                            "offset": message.offset,
                            "timestamp": message.timestamp,
                            "value": decode_message(message.value),
                        }
                    )
        for topic in collected:
            collected[topic].sort(key=lambda row: (row["timestamp"], row["offset"]))
            collected[topic] = collected[topic][-limit:]
            collected[topic].reverse()
        return {
            topic: {"count": counts.get(topic, 0), "messages": collected.get(topic, [])}
            for topic in TOPICS
        }
    finally:
        consumer.close()


def snapshot():
    errors = {}
    try:
        creatio = creatio_rows()
    except Exception as exc:
        creatio = []
        errors["creatio"] = str(exc)
    try:
        sap = sap_rows()
    except Exception as exc:
        sap = []
        errors["sap"] = str(exc)
    try:
        directum = directum_rows()
    except Exception as exc:
        directum = []
        errors["directum"] = str(exc)
    try:
        topics = read_topics()
    except Exception as exc:
        topics = {}
        errors["topics"] = str(exc)
    try:
        sap_packets = inbox_rows(SAP_URL)
    except Exception as exc:
        sap_packets = []
        errors["sapPackets"] = str(exc)
    try:
        directum_packets = inbox_rows(DIRECTUM_URL)
    except Exception as exc:
        directum_packets = []
        errors["directumPackets"] = str(exc)
    try:
        delivery = delivery_rows()
    except Exception as exc:
        delivery = []
        errors["deliveryState"] = str(exc)
    return {
        "creatio": creatio,
        "sap": sap,
        "directum": directum,
        "sapPackets": sap_packets,
        "directumPackets": directum_packets,
        "deliveryState": delivery,
        "topics": topics,
        "errors": errors,
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print("%s %s" % (self.address_string(), fmt % args), flush=True)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/snapshot":
            with lock:
                self._send(200, snapshot())
            return
        if path in ("/", "/index.html"):
            with open(PAGE, "rb") as handle:
                body = handle.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/clear":
            self._clear_all()
            return
        if path == "/api/deliver/sap":
            self._deliver("sap")
            return
        if path == "/api/deliver/directum":
            self._deliver("directum")
            return
        if path == "/api/deliver/directum/down":
            self._directum_down()
            return
        if path == "/api/deliver/directum/up":
            self._directum_up()
            return
        if path == "/api/deliver/directum/data-error":
            self._directum_data_error()
            return
        if path == "/api/replay/directum":
            self._replay("directum")
            return
        if path == "/api/replay/sap":
            self._replay("sap")
            return
        if path == "/api/deliver/directum/reject":
            body = self._read_json()
            if body is None:
                return
            self._deliver_directum_reject(body.get("code") or "")
            return
        account = self._read_json()
        if account is None:
            return
        if path == "/api/accounts":
            status, payload = http_json("POST", CREATIO_URL + "/accounts?publish=0", account)
            self._send(status, payload)
            return
        if path == "/api/publish":
            account_id = (account or {}).get("Id") or ""
            if not account_id:
                self._send(400, {"error": "Id is required"})
                return
            status, saved = http_json(
                "GET",
                CREATIO_URL + "/accounts/" + urllib.parse.quote(account_id, safe=""),
            )
            if status != 200 or not isinstance(saved, dict):
                detail = saved if isinstance(saved, dict) else {"error": "Creatio не отдал карточку"}
                self._send(status if status >= 400 else 502, detail)
                return
            status, payload = http_json("POST", CAMEL_URL + "/ingress/accounts", saved, timeout=30)
            self._send(status, payload if isinstance(payload, dict) else {"event": payload})
            return
        self._send(404, {"error": "not found"})

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b""
        try:
            return json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid json"})
            return None

    def _deliver(self, system):
        url = SAP_URL if system == "sap" else DIRECTUM_URL
        group = "ibus-sap" if system == "sap" else "ibus-directum"
        try:
            before = len(inbox_rows(url))
        except Exception as exc:
            self._send(502, {"error": "inbox unavailable", "detail": str(exc)})
            return
        lagging = not self._caught_up(group, "crm.account.upserted.v1")
        status, payload = http_json("POST", CAMEL_URL + "/control/%s/start" % system, {}, timeout=30)
        if status >= 400:
            self._send(status, payload if isinstance(payload, dict) else {"error": "camel start failed", "detail": payload})
            return
        deadline = time.time() + 35
        fresh = []
        while time.time() < deadline:
            try:
                rows = inbox_rows(url)
            except Exception:
                rows = []
            if len(rows) > before:
                fresh = rows[before:]
                break
            if lagging and self._caught_up(group, "crm.account.upserted.v1"):
                time.sleep(0.4)
                try:
                    rows = inbox_rows(url)
                except Exception:
                    rows = []
                if len(rows) > before:
                    fresh = rows[before:]
                break
            time.sleep(0.4)
        if fresh:
            time.sleep(1)
        http_json("POST", CAMEL_URL + "/control/%s/stop" % system, {}, timeout=30)
        if fresh:
            self._send(200, {"status": "delivered", "packets": fresh})
            return
        if lagging and self._caught_up(group, "crm.account.upserted.v1"):
            self._send(200, {"status": "skipped", "packets": []})
            return
        self._send(200, {"status": "empty", "packets": []})

    def _directum_down(self):
        status, payload = http_json("POST", DIRECTUM_URL + "/pilot/down", {})
        if status >= 400:
            self._send(status, payload if isinstance(payload, dict) else {"error": "directum down failed"})
            return
        status, payload = http_json("POST", CAMEL_URL + "/control/directum/start", {}, timeout=30)
        if status >= 400:
            self._send(status, payload if isinstance(payload, dict) else {"error": "camel start failed", "detail": payload})
            return
        time.sleep(3)
        self._send(200, {"status": "waiting"})

    def _directum_up(self):
        http_json("POST", DIRECTUM_URL + "/pilot/up", {})
        status, payload = http_json("POST", CAMEL_URL + "/control/directum/start", {}, timeout=30)
        if status >= 400:
            self._send(status, payload if isinstance(payload, dict) else {"error": "camel start failed", "detail": payload})
            return
        caught = self._wait_caught_up("ibus-directum", "crm.account.upserted.v1", 40)
        time.sleep(0.5)
        http_json("POST", CAMEL_URL + "/control/directum/stop", {}, timeout=30)
        try:
            rows = directum_rows()
        except Exception:
            rows = []
        self._send(200, {"status": "delivered" if caught else "timeout", "directum": rows})

    def _directum_data_error(self):
        before = self._topic_count("dlq.directum.account")
        http_json("POST", DIRECTUM_URL + "/pilot/data-error", {})
        after = before
        try:
            status, payload = http_json("POST", CAMEL_URL + "/control/directum/start", {}, timeout=30)
            if status >= 400:
                self._send(status, payload if isinstance(payload, dict) else {"error": "camel start failed", "detail": payload})
                return
            deadline = time.time() + 20
            while time.time() < deadline:
                after = self._topic_count("dlq.directum.account")
                if after > before:
                    break
                time.sleep(0.4)
            time.sleep(1)
        finally:
            http_json("POST", DIRECTUM_URL + "/pilot/data-ok", {})
            http_json("POST", CAMEL_URL + "/control/directum/stop", {}, timeout=30)
        status_name = "data-error" if after > before else "empty"
        self._send(200, {"status": status_name, "before": before, "after": after})

    def _replay(self, system):
        source = "dlq.%s.account" % system
        target = "replay.%s.account" % system
        group = "ibus-%s-replay" % system
        record = self._last_raw(source)
        if record is None:
            self._send(409, {"error": "В списке ошибок пусто"})
            return
        before = self._topic_end(target)
        try:
            self._produce(target, record["key"], record["value"])
        except Exception as exc:
            self._send(502, {"error": "replay publish failed", "detail": str(exc)})
            return
        status, payload = http_json("POST", CAMEL_URL + "/control/%s/start" % system, {}, timeout=30)
        if status >= 400:
            self._send(status, payload if isinstance(payload, dict) else {"error": "camel start failed", "detail": payload})
            return
        caught = self._wait_committed(group, target, before + 1, 20)
        time.sleep(0.5)
        http_json("POST", CAMEL_URL + "/control/%s/stop" % system, {}, timeout=30)
        self._send(200, {"status": "replayed" if caught else "timeout"})

    def _deliver_directum_reject(self, code):
        # Every Directum write for this Creatio id returns an error until the
        # reader stops. A normal message already sitting in the topic must not
        # change the card on the way to the error topic.
        http_json("POST", DIRECTUM_URL + "/pilot/reject", {"code": code})
        try:
            self._deliver("directum")
        finally:
            http_json("POST", DIRECTUM_URL + "/pilot/release", {"code": code})

    def _clear_all(self):
        errors = []
        for system in ("sap", "directum"):
            try:
                http_json("POST", CAMEL_URL + "/control/%s/stop" % system, {}, timeout=30)
            except Exception as exc:
                errors.append("camel %s: %s" % (system, exc))
        for url in (CREATIO_URL, SAP_URL, DIRECTUM_URL):
            try:
                status, payload = http_json("POST", url + "/clear", {})
                if status != 200:
                    errors.append(f"{url}: {status}")
            except Exception as exc:
                errors.append(f"{url}: {exc}")
        try:
            status, payload = http_json("POST", CAMEL_URL + "/delivery-state/clear", {})
            if status != 200:
                errors.append("delivery_state: %s" % status)
        except Exception as exc:
            errors.append("delivery_state: %s" % exc)
        try:
            self._reset_topics()
        except Exception as exc:
            errors.append(f"topics: {exc}")
        try:
            self._truncate_container_logs()
        except Exception as exc:
            errors.append(f"logs: {exc}")
        if errors:
            self._send(500, {"status": "partial", "errors": errors})
        else:
            self._send(200, {"status": "cleared"})

    def _topic_bounds(self, topic):
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKERS],
            group_id=None,
            enable_auto_commit=False,
            request_timeout_ms=8000,
            api_version_auto_timeout_ms=8000,
        )
        try:
            parts = consumer.partitions_for_topic(topic) or set()
            tps = [TopicPartition(topic, part) for part in sorted(parts)]
            if not tps:
                return [], {}, {}
            return tps, consumer.beginning_offsets(tps), consumer.end_offsets(tps)
        finally:
            consumer.close()

    def _topic_end(self, topic):
        _tps, _starts, ends = self._topic_bounds(topic)
        return sum(ends.values())

    def _topic_count(self, topic):
        tps, starts, ends = self._topic_bounds(topic)
        return sum(ends[tp] - starts[tp] for tp in tps)

    def _committed_sum(self, group, topic):
        admin = KafkaAdminClient(bootstrap_servers=[BROKERS], request_timeout_ms=8000)
        try:
            try:
                committed = admin.list_consumer_group_offsets(group)
            except Exception:
                return 0
            total = 0
            for tp, meta in (committed or {}).items():
                if getattr(tp, "topic", None) == topic and meta is not None:
                    total += meta.offset
            return total
        finally:
            admin.close()

    def _caught_up(self, group, topic):
        end = self._topic_end(topic)
        if end <= 0:
            return True
        return self._committed_sum(group, topic) >= end

    def _wait_caught_up(self, group, topic, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._caught_up(group, topic):
                return True
            time.sleep(0.4)
        return False

    def _wait_committed(self, group, topic, expected, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._committed_sum(group, topic) >= expected:
                return True
            time.sleep(0.4)
        return False

    def _last_raw(self, topic):
        consumer = KafkaConsumer(
            bootstrap_servers=[BROKERS],
            group_id=None,
            enable_auto_commit=False,
            consumer_timeout_ms=800,
            request_timeout_ms=8000,
            api_version_auto_timeout_ms=8000,
        )
        try:
            parts = consumer.partitions_for_topic(topic) or set()
            tps = [TopicPartition(topic, part) for part in sorted(parts)]
            if not tps:
                return None
            consumer.assign(tps)
            ends = consumer.end_offsets(tps)
            starts = consumer.beginning_offsets(tps)
            chosen = None
            for tp in tps:
                if ends[tp] <= starts[tp]:
                    continue
                consumer.seek(tp, ends[tp] - 1)
                batch = consumer.poll(timeout_ms=1000, max_records=5)
                for messages in batch.values():
                    for message in messages:
                        if message.topic != topic:
                            continue
                        if chosen is None or (message.timestamp, message.offset) >= (chosen.timestamp, chosen.offset):
                            chosen = message
            if chosen is None:
                return None
            return {"key": chosen.key, "value": chosen.value}
        finally:
            consumer.close()

    def _produce(self, topic, key, value):
        producer = KafkaProducer(
            bootstrap_servers=[BROKERS],
            acks="all",
            retries=5,
            request_timeout_ms=15000,
        )
        try:
            producer.send(topic, key=key, value=value).get(timeout=15)
        finally:
            producer.close()

    def _reset_topics(self):
        topics = list(TOPICS)
        admin = KafkaAdminClient(bootstrap_servers=[BROKERS])
        try:
            try:
                admin.delete_topics(topics)
            except UnknownTopicOrPartitionError:
                pass
            deadline = time.time() + 10
            while time.time() < deadline:
                existing = set(admin.list_topics())
                if not existing.intersection(topics):
                    break
                time.sleep(0.5)
            for topic in topics:
                admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
        finally:
            admin.close()

    def _truncate_container_logs(self):
        names = ["ibus-creatio", "ibus-sap-b1", "ibus-directum-rx", "ibus-camel", "ibus-stand"]
        for name in names:
            try:
                log_path = subprocess.check_output(
                    ["sudo", "-n", "docker", "inspect", "-f", "{{.LogPath}}", name],
                    text=True,
                ).strip()
                if log_path:
                    subprocess.run(["sudo", "-n", "truncate", "-s", "0", log_path], check=True)
            except Exception:
                pass

    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
