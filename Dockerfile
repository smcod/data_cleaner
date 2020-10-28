FROM python:3.7-slim-stretch

RUN groupadd --system ml_services && useradd --system -g ml_services data_cleaner

RUN apt-get update ; \
    apt-get install -y gcc

RUN mkdir -p /opt/data_cleaner
ENV APP_DIR /opt/data_cleaner
WORKDIR $APP_DIR

COPY src $APP_DIR/src/
RUN pip3 install --upgrade pip && pip3 install -r src/requirements.txt

USER data_cleaner

CMD ["python3", "-m", "src.main"]
