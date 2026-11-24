FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# HTTP API（/docs、/offset、/status）与探头 TCP 长连接端口
EXPOSE 8000 9000

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD python -c "import json,urllib.request,sys; \
sys.exit(0 if json.load(urllib.request.urlopen('http://127.0.0.1:8000/health'))['ok'] else 1)"

CMD ["python", "-m", "app.main"]
