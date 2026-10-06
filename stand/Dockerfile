FROM python:3.12-alpine
WORKDIR /app
RUN pip install --no-cache-dir kafka-python-ng==2.2.3
COPY app.py index.html ./
EXPOSE 8080
CMD ["python", "-u", "app.py"]
