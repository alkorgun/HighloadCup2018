# Список допилов

## Что пока не обрабатывается

* /accounts/filter:
  * premium_*
  * likes_contains
* /accounts/groups:
  * keys=interests
  * likes

## Костыли

* `fixArray(values[0])`
* `line = bytes.Replace(line, []byte("_cnt"), []byte("count"), 1)`
* `line = bytes.Replace(bytes.Replace(bytes.Replace(line, []byte("\"city\":\"\""), []byte(""), 50), []byte(",}"), []byte("}"), 50), []byte(",,"), []byte(","), 50)`
* `line = bytes.Replace(line, []byte("\"country\":\"\""), []byte("\"country\":\null"), 1)`
* `//json = bytes.Replace(json, []byte("\n"), []byte(""), 9000)`

## Проблемы

* Что-то не так с верными ответами на группировках.

## В принципе не сделано

* **GET**: `/accounts/<id>/recommend/`
* **GET**: `/accounts/<id>/suggest/`
* **POST**: `/accounts/likes/`
* **POST**: `/accounts/<id>/`
