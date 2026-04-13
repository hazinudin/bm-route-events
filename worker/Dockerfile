# File: worker/Dockerfile

FROM python:3.11-slim

WORKDIR /app

COPY ../wheels ./wheels
RUN pip install ./wheels/*.whl

COPY ../requirements.txt .
RUN grep -v '^\.\.\/bm-lrs\/lrs-client' requirements.txt > requirements_docker.txt && \
    pip install -r requirements_docker.txt && \
    rm requirements_docker.txt

COPY ../src ./src
COPY ../setup.py .
COPY ../worker ./worker

RUN pip install .
# For finding memory leak
# RUN pip install memray

ENV PYTHONPATH=/app/source/src
# ENV PYTHONMALLOC=malloc

# CMD ["memray", "run",  "-o", "output.bin", "worker/worker.py"]
CMD ["python", "worker/worker.py"]
