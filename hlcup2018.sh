#!/bin/bash

service clickhouse-server start

# sleep 10

export PYTHONIOENCODING=utf-8

echo -e "\nfiles found:\n$( ls -1 /tmp/data )\n"

unzip -o /tmp/data/data.zip

echo -e "\ncreating schema..."

cat /var/lib/hlc2018/schema/db.sql | clickhouse-client --multiline
cat /var/lib/hlc2018/schema/accounts.sql | clickhouse-client --multiline
cat /var/lib/hlc2018/schema/likes.sql | clickhouse-client --multiline

echo -e "\nloading data..."

loader.py accounts_*.json | clickhouse-client --query="INSERT INTO hlcup2018.accounts FORMAT CSV"
# likes_loader.py likes_*.json | clickhouse-client --query="INSERT INTO hlcup2018.likes FORMAT CSV"

echo -e "\nclearing...\n"

/bin/hlcup2018 -port 80
