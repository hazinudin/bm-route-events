# File: worker/Dockerfile

FROM python:3.11-slim

# Install git + build tools (if needed for some Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ARG CACHE_BUST=1
RUN git clone --depth 1 --branch master https://github.com/hazinudin/bm-route-events.git .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install .

ENV PYTHONPATH=/app/src

CMD ["python", "worker/worker.py"]
