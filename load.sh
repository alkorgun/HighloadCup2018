#!/bin/bash

d=$(pwd)

echo -e "\ncreating schema..."

cat schema/db.sql | clickhouse-client --multiline
cat schema/accounts.sql | clickhouse-client --multiline
cat schema/likes.sql | clickhouse-client --multiline

echo -e "\nloading data...\n"

cd data

$d/loaders/loader.py accounts_*.json | clickhouse-client --query="INSERT INTO hlcup2018.accounts FORMAT CSV"
$d/loaders/likes_loader.py likes_*.json | clickhouse-client --query="INSERT INTO hlcup2018.likes FORMAT CSV"

cd -
