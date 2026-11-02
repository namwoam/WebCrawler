FROM python:3.12-slim
WORKDIR /app
COPY containers/api/requirements.txt /tmp/api-requirements.txt
COPY poc/distributed/requirements.txt /tmp/poc-requirements.txt
RUN pip install --no-cache-dir -r /tmp/api-requirements.txt -r /tmp/poc-requirements.txt
COPY containers/api /app/containers/api
COPY libs /app/libs
COPY poc/distributed /app/poc/distributed
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
CMD ["python", "-m", "poc.distributed.app.worker"]
