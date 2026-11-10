# syntax=docker/dockerfile:1.7
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=Europe/Moscow

WORKDIR /app

# Зависимости отдельным слоем — кэш не инвалидируется при правках кода.
# apt-get не нужен: ca-certificates и tzdata уже в python:3.12-slim,
# а healthcheck сделан на Python (см. ниже).
COPY requirements-server.txt .
RUN pip install -r requirements-server.txt

# Код проекта
COPY app/                ./app/
COPY rest_server.py      ./
COPY server_main.py      ./
COPY version.py          ./
COPY contract_v1.md      ./
COPY stage1_contract.md  ./

# Непривилегированный пользователь
RUN useradd --create-home --uid 10001 appuser \
 && mkdir -p /app/logs /app/config \
 && chown -R appuser:appuser /app
USER appuser

VOLUME ["/app/logs", "/app/config"]

EXPOSE 8080

# HEALTHCHECK без curl — через Python (stdlib)
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3).status==200 else 1)"

CMD ["python", "server_main.py"]