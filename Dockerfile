FROM python:3.9-alpine

WORKDIR /code

COPY . .

RUN pip3 install -r requirements.txt

CMD [ "python3", "ygg-autofeed" ]