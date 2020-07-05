FROM python:3.7.7-alpine3.11

WORKDIR /usr/src/app

COPY ./requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "./timestamp-service.py"]
