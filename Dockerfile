FROM ubuntu:18.04
LABEL author=alkorgun@gmail.com

RUN apt update && apt install -y gnupg software-properties-common

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4
RUN apt-add-repository "deb http://repo.yandex.ru/clickhouse/deb/stable/ main/"

RUN apt update && apt install -y zip python3.6 clickhouse-server clickhouse-client

ADD hlcup2018/hlcup2018 /bin/
ADD loaders/* /bin/
ADD hlcup2018.sh /bin/
RUN chmod a+x /bin/*

RUN mkdir -p /tmp/data
RUN mkdir -p /var/lib/hlc2018/schema

ADD schema/* /var/lib/hlc2018/schema/

WORKDIR /root

EXPOSE 80

CMD /bin/hlcup2018.sh
