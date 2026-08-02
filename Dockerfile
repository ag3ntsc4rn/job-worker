FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY pyproject.toml .
COPY src ./src
# Installed rather than copied, so `worker` imports from site-packages: a script
# run from /app/scripts gets its own directory on sys.path, not /app.
RUN pip install --no-cache-dir --no-deps .

COPY scripts ./scripts

# Run as a non-root user; the worker needs nothing on disk.
RUN useradd --create-home --uid 10001 worker
USER worker

CMD ["python", "-m", "worker"]
