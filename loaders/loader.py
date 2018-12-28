#!/usr/bin/env python3

# loader.py accounts_*.json | clickhouse-client --query="INSERT INTO hlcup2018.accounts FORMAT CSV"

import json
import re
import sys
import time

ROW_TEMPLATE = (
	"{r[id]},"
	"{r[email]},"
	"{r[fname]},"
	"{r[sname]},"
	"\"{r[interests]}\","
	"{r[status]},"
	"{r[premium][start]},"
	"{r[premium][finish]},"
	"{r[sex]},"
	"{r[phone]},"
	"{r[phone_c]},"
	"{r[birth]},"
	"{r[birth_y]},"
	"{r[city]},"
	"{r[country]},"
	"{r[joined]},"
	"{r[joined_y]}"
)


class Proxy(object):
	_row = {} # fuck linter
	_premium = dict(start='""', finish='""')
	_phone = re.compile(r"\((\d{3})\)")
	def __getitem__(self, key):
		try:
			return self._row[key]
		except KeyError:
			if key == "premium":
				return self._premium
			if key == "interests":
				return "[]"
			if key == "birth_y":
				b = self["birth"]
				if not b:
					return 0
				return time.gmtime(int(b))[0]
			if key == "joined_y":
				b = self["joined"]
				if not b:
					return 0
				return time.gmtime(int(b))[0]
			if key == "phone_c":
				c = self._phone.search(self["phone"])
				if c:
					return c.group(1)
			return '""'


p = Proxy()

if __name__ == "__main__":
	likes = {}
	for i, fn in enumerate(sys.argv[1:], 1):
		with open(fn) as fp:
			data = json.load(fp)
		data = data.pop("accounts")

		for r in data:
			l = r.get("likes", None)
			if l:
				likes[r["id"]] = l
			p._row = r
			print(ROW_TEMPLATE.format(r=p))

			sys.stdout.flush()

		with open("likes_%d.json" % i, "w") as fp:
			json.dump(likes, fp)

		likes.clear()
