FROM python:3.7.7-alpine3.11

WORKDIR /usr/src/app

RUN apk add gcc musl-dev

COPY ./requirements.txt ./

RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-u", "./capture_stream_metadata.py"]
