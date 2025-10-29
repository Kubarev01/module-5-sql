FROM python:3.11-slim


RUN apt-get update && apt-get install -y gcc libpq-dev build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app


COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt


COPY . /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app


CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]