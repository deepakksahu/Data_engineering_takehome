FROM python:3.7

USER root

COPY . .

RUN pip install -r requirements.txt