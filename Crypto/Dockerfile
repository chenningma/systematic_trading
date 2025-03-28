FROM --platform=linux/amd64 python:3.9.10 AS build

WORKDIR /Crypto 
COPY . /Crypto

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "crypto_download_px_daily.py"]