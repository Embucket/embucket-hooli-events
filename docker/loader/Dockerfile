FROM python:3.12-slim

WORKDIR /app

# Install runtime deps only (test deps are excluded by filtering).
COPY simulator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
      $(grep -v -E '^(pytest|pytest-asyncio)' /app/requirements.txt)

COPY simulator/simulate.py /app/simulate.py

# Defaults; overridable via ECS task env.
ENV SESSIONS_PER_MIN=60 \
    CONCURRENCY=20 \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/app/simulate.py"]
