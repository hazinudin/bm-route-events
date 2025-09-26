# File: worker/Dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY ../requirements.txt .
RUN pip install -r requirements.txt

COPY ../src ./src
COPY ../setup.py .
COPY ../worker ./worker

RUN pip install .
# For finding memory leak
RUN pip install memray

ENV PYTHONPATH=/app/source/src
ENV PYTHONMALLOC=malloc

CMD ["memray", "run",  "-o", "output.bin", "worker/worker.py"]
# CMD ["python", "worker/worker.py"]
