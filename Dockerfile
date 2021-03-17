FROM python:3.9-alpine

WORKDIR /code

ADD requirements.txt requirements.txt

ADD ygg-autofeed.py ygg-autofeed.py

RUN pip3 install -r requirements.txt

VOLUME /blackhole

CMD [ "python3", "ygg-autofeed" ]