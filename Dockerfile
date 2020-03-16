FROM python:3.7.4-stretch

USER root

COPY . .

RUN pip install -r requirements.txt
