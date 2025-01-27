FROM python:3.12.8-slim

WORKDIR /analytics

RUN apt-get update; \
    apt-get -y install libpq-dev gcc python3-dev postgresql-contrib

COPY src/ runner/
COPY requirements.txt requirements.txt

RUN pip install --upgrade pip;\
    pip install -r requirements.txt;\
    rm requirements.txt

WORKDIR /analytics/runner