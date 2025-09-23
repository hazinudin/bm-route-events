# File: worker/Dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY ../requirements.txt .
RUN pip install -r requirements.txt

COPY ../src ./src
COPY ../setup.py .
COPY ../worker ./worker

RUN pip install .

ENV PYTHONPATH=/app/source/src

CMD ["python", "worker/worker.py"]
