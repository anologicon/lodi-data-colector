FROM python:3.10-slim
RUN apt-get update \
&& apt-get clean
WORKDIR app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt