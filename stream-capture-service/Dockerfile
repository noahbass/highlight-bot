FROM python:3.7.7-slim

WORKDIR /usr/src/app

RUN apt-get update &&\
    apt-get install -y g++ libsm6 tesseract-ocr libtesseract-dev libleptonica-dev

COPY ./requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "./main.py"]
