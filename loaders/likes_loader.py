#!/usr/bin/env python3

# likes_loader.py likes_*.json | clickhouse-client --query="INSERT INTO hlcup2018.likes FORMAT CSV"

import json
import re
import sys
import time

ROW_TEMPLATE = (
	"{k},"
	"{r[id]},"
	"{r[ts]}"
)

if __name__ == "__main__":
	for fn in sys.argv[1:]:
		with open(fn) as fp:
			data = json.load(fp)

		for k, likes in data.items():
			for r in likes:
				print(ROW_TEMPLATE.format(k=k, r=r))

				sys.stdout.flush()
