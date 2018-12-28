package main

import (
	//	"database/sql"
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"
	// _ "github.com/kshvakov/clickhouse"
)

type chanInc struct {
	ch chan bool
	pb *[]byte
}

var csvQueue chan *chanInc
var csvTrigger chan bool

var queueIters uint32

var pSQL bool

type account struct {
	id      int32
	email   string
	country string
	status  string
	birth   int32
}

func fixArray(str string) string {
	return strings.Join(strings.Split(str, ","), "','")
}

func set400(ctx *routing.Context) {
	ctx.SetStatusCode(400)
	ctx.SetContentType("application/json")
	ctx.Response.Header.Set("Content-Length", "0")
}

// /accounts/filter/?
func accountsFilter(ctx *routing.Context) error {
	columns := map[string]bool{
		"id":    true,
		"email": true,
	}
	var where []string
	var eq string
	var wq string
	var err error

	//connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000")

	limit := 9000000

	q, _ := url.ParseQuery(ctx.QueryArgs().String())

	for key, values := range q {
		switch key {
		case "sex_eq":
			columns["sex"] = true
			where = append(where, fmt.Sprintf("sex = '%s'", values[0]))
		case "email_domain":
			columns["email"] = true
			where = append(where, fmt.Sprintf("email like '%%@%s'", values[0]))
		case "email_lt":
			columns["email"] = true
			where = append(where, fmt.Sprintf("email < '%s'", values[0]))
		case "email_gt":
			columns["email"] = true
			where = append(where, fmt.Sprintf("email > '%s'", values[0]))
		case "status_eq":
			columns["status"] = true
			where = append(where, fmt.Sprintf("status = '%s'", values[0]))
		case "status_neq":
			columns["status"] = true
			where = append(where, fmt.Sprintf("status != '%s'", values[0]))
		case "fname_eq":
			columns["fname"] = true
			where = append(where, fmt.Sprintf("fname = '%s'", values[0]))
		case "fname_any":
			columns["fname"] = true
			where = append(where, fmt.Sprintf("has(['%s'], fname) = 1", fixArray(values[0]))) // fix
		case "fname_null":
			if values[0] == "1" {
				eq = ""
			} else {
				columns["fname"] = true
				eq = "!"
			}
			where = append(where, fmt.Sprintf("fname %s= ''", eq))
		case "sname_eq":
			columns["sname"] = true
			where = append(where, fmt.Sprintf("sname = '%s'", values[0]))
		case "sname_starts":
			columns["sname"] = true
			where = append(where, fmt.Sprintf("sname like '%s%%'", values[0]))
		case "sname_null":
			if values[0] == "1" {
				eq = ""
			} else {
				columns["sname"] = true
				eq = "!"
			}
			where = append(where, fmt.Sprintf("sname %s= ''", eq))
		case "phone_code":
			columns["phone"] = true
			where = append(where, fmt.Sprintf("phone_c = '%s'", values[0]))
		case "phone_null":
			if values[0] == "1" {
				eq = ""
			} else {
				columns["phone"] = true
				eq = "!"
			}
			where = append(where, fmt.Sprintf("phone %s= ''", eq))
		case "country_eq":
			columns["country"] = true
			where = append(where, fmt.Sprintf("country = '%s'", values[0]))
		case "country_null":
			if values[0] == "1" {
				eq = ""
			} else {
				columns["country"] = true
				eq = "!"
			}
			where = append(where, fmt.Sprintf("country %s= ''", eq))
		case "city_eq":
			columns["city"] = true
			where = append(where, fmt.Sprintf("city = '%s'", values[0]))
		case "city_any":
			columns["city"] = true
			where = append(where, fmt.Sprintf("has(['%s'], city) = 1", fixArray(values[0]))) // fix
		case "city_null":
			if values[0] == "1" {
				eq = ""
			} else {
				columns["city"] = true
				eq = "!"
			}
			where = append(where, fmt.Sprintf("city %s= ''", eq))
		case "birth_lt":
			columns["birth"] = true
			where = append(where, fmt.Sprintf("birth < %s", values[0]))
		case "birth_gt":
			columns["birth"] = true
			where = append(where, fmt.Sprintf("birth > %s", values[0]))
		case "birth_year":
			columns["birth"] = true
			where = append(where, fmt.Sprintf("birth_y = %s", values[0]))
		case "interests_contains":
			//columns["interests"] = true
			where = append(where, fmt.Sprintf("hasAll(interests, ['%s']) = 1", fixArray(values[0]))) // fix
		case "interests_any":
			//columns["interests"] = true
			where = append(where, fmt.Sprintf("hasAny(interests, ['%s']) = 1", fixArray(values[0]))) // fix
		case "likes_contains":
			columns["likes"] = true
			ctx.SetContentType("application/json")
			ctx.Response.Header.Set("Content-Length", "15")

			ctx.Write([]byte("{\"accounts\":[]}"))
			return nil // fix
		case "premium_now":
			//columns["premium"] = true // fix
			where = append(where, "cast(now() as int) between p_start and p_end")

			ctx.SetStatusCode(501)
			return nil
		case "premium_null":
			//columns["premium"] = true // fix
			if values[0] == "1" {
				eq = ""
			} else {
				eq = "!"
			}
			where = append(where, fmt.Sprintf("p_start %s= 0", eq))

			ctx.SetStatusCode(501)
			return nil
		case "query_id":
			continue
		case "limit":
			limit, err = strconv.Atoi(values[0])
			if err != nil {
				set400(ctx)
				return nil
			}
		default:
			set400(ctx)
			return nil
		}
	}

	coli := make([]string, len(columns))

	i := 0
	for k := range columns {
		coli[i] = k
		i++
	}

	cols := strings.Join(coli, ", ")

	if len(where) > 0 {
		wq = fmt.Sprintf(" where %s", strings.Join(where, " and "))
	} else {
		wq = ""
	}

	query := fmt.Sprintf("select %s from hlcup2018.accounts%s order by id desc limit %d format JSONEachRow;", cols, wq, limit)
	if pSQL {
		fmt.Println(query)
	}

	r, err := http.Get(fmt.Sprintf("http://localhost:8123/?query=%s", url.QueryEscape(query)))
	if err != nil {
		ctx.SetStatusCode(500)
		return nil
	}

	ctx.SetContentType("application/json")

	var buf []byte
	b := bytes.NewBuffer(buf)

	b.Write([]byte("{\"accounts\":["))

	f := 0

	for buff := bufio.NewReader(r.Body); ; {
		line, _, err := buff.ReadLine()
		if err != nil {
			break
		}

		if f > 0 {
			b.Write([]byte{','})
		} else {
			f = 1
		}

		b.Write(line)

		//acc := account{}

		//json.Unmarshal(line, acc)
	}

	b.Write([]byte("]}"))

	ctx.Response.Header.Set("Content-Length", strconv.Itoa(b.Len()))

	io.Copy(ctx, b)
	return nil
}

// /accounts/group/?
func accountsGroup(ctx *routing.Context) error {
	var keys []string
	var interests string
	var where []string
	var ts []string
	var wq string
	var err error

	limit := 50
	order := "desc"

	q, _ := url.ParseQuery(ctx.QueryArgs().String())

	for key, values := range q {
		switch key {
		case "keys":
			keys = strings.Split(values[0], ",")
			for _, k := range keys {
				switch k {
				case "interests":
					ctx.SetStatusCode(501)
					return nil
				case "sex", "status", "country", "city": //, "interests":
					continue
				default:
					set400(ctx)
					return nil
				}
			}
		case "order":
			switch values[0] {
			case "1":
				order = "asc"
			case "-1":
				order = "desc"
			default:
				set400(ctx)
				return nil
			}
		case "limit":
			limit, err = strconv.Atoi(values[0])
			if err != nil {
				set400(ctx)
				return nil
			}
		case "query_id":
			continue
		default:
			switch key {
			case "likes":
				ctx.SetStatusCode(501)
				return nil
			case "birth":
				where = append(where, fmt.Sprintf("birth_y = %s", values[0]))
			case "joined":
				where = append(where, fmt.Sprintf("joined_y = %s", values[0]))
			case "interests":
				interests = values[0]
				where = append(where, fmt.Sprintf("has(interests, '%s') = 1", values[0]))
			case "sex", "status", "city", "country", "email", "fname", "sname", "phone":
				where = append(where, fmt.Sprintf("%s = '%s'", key, values[0]))
			default:
				set400(ctx)
				return nil
			}
		}
	}

	for _, s := range keys {
		if s == "interests" && interests != "" {
			s = fmt.Sprintf("'%s' as 'interests'", interests)
		}
		ts = append(ts, s)
	}
	ks := fmt.Sprintf(", %s", strings.Join(ts, ", "))
	jk := fmt.Sprintf(", %s %s", strings.Join(keys, fmt.Sprintf(" %s, ", order)), order)
	gs := strings.Join(keys, ", ")

	if len(where) > 0 {
		wq = fmt.Sprintf(" where %s", strings.Join(where, " and "))
	} else {
		wq = ""
	}

	query := fmt.Sprintf("select toInt32(count()) as _cnt%s from hlcup2018.accounts%s group by %s order by _cnt %s%s limit %d format JSONEachRow;", ks, wq, gs, order, jk, limit)
	if pSQL {
		fmt.Println(query)
	}

	r, err := http.Get(fmt.Sprintf("http://localhost:8123/?query=%s", url.QueryEscape(query)))
	if err != nil {
		ctx.SetStatusCode(500)
		return nil
	}

	ctx.SetContentType("application/json")

	lks := len(keys)

	var buf []byte
	b := bytes.NewBuffer(buf)

	b.Write([]byte("{\"groups\":["))

	f := 0

	for buff := bufio.NewReader(r.Body); ; {
		line, _, err := buff.ReadLine()
		if err != nil {
			break
		}

		if f > 0 {
			b.Write([]byte{','})
		} else {
			f = 1
		}

		line = bytes.Replace(line, []byte("_cnt"), []byte("count"), 1)
		line = bytes.Replace(bytes.Replace(bytes.Replace(line, []byte("\"city\":\"\""), []byte(""), 50), []byte(",}"), []byte("}"), 50), []byte(",,"), []byte(","), 50)
		if lks > 1 {
			line = bytes.Replace(bytes.Replace(bytes.Replace(line, []byte("\"country\":\"\""), []byte(""), 50), []byte(",}"), []byte("}"), 50), []byte(",,"), []byte(","), 50)
		} else {
			line = bytes.Replace(line, []byte("\"country\":\"\""), []byte("\"country\":null"), 50)
		}

		b.Write(line)

		//acc := account{}

		//json.Unmarshal(line, acc)
	}

	b.Write([]byte("]}"))

	ctx.Response.Header.Set("Content-Length", strconv.Itoa(b.Len()))

	io.Copy(ctx, b)
	return nil
}

func accountsNew(ctx *routing.Context) error {
	body := ctx.PostBody()
	cpbody := make([]byte, len(body))

	copy(cpbody, body)

	ch := make(chan bool)
	st := &chanInc{ch, &cpbody}

	csvQueue <- st

	<-ch

	ctx.SetStatusCode(201)
	ctx.SetContentType("application/json")
	ctx.Response.Header.Set("Content-Length", "2")
	ctx.Write([]byte("{}"))
	return nil
}

func accountsFakePOST(ctx *routing.Context) error {
	ctx.SetStatusCode(202)
	ctx.SetContentType("application/json")
	ctx.Response.Header.Set("Content-Length", "2")
	ctx.Write([]byte("{}"))
	return nil
}

func accountsFake(ctx *routing.Context) error {
	ctx.SetStatusCode(400)
	ctx.SetContentType("application/json")
	ctx.Response.Header.Set("Content-Length", "0")
	return nil
}

/* func endpointSwitch(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/accounts/filter/":
		accountsFilter(ctx)
	case "/accounts/group/":
		accountsGroup(ctx)
	case "/accounts/new/":
		accountsNew(ctx)
	case "/accounts/likes/":
		accountsFakePOST(ctx)
	case "/accounts/":
		if ctx.IsPost() {
			accountsFakePOST(ctx)
		} else {
			accountsFake(ctx)
		}
	default:
		ctx.Error("not found", 404)
	}
} */

func getRouter() *routing.Router {
	router := routing.New()
	router.Get("/accounts/filter/", accountsFilter)
	router.Get("/accounts/group/", accountsGroup)
	router.Post("/accounts/new/", accountsNew)
	router.Post("/accounts/likes/", accountsFakePOST)
	router.Post("/accounts/<id>/", accountsFakePOST)
	router.Get("/accounts/<id>/recommend/", accountsFake)
	router.Get("/accounts/<id>/suggest/", accountsFake)
	return router
}

func pushQueue(pbuff *[][]byte) {
	var iters int
	var err error

	buff := *pbuff

	l := len(buff)

	loader := exec.Command("new_loader.py")

	stdin, _ := loader.StdinPipe()
	stdout, _ := loader.StdoutPipe()
	stderr, _ := loader.StderrPipe()

	br := bufio.NewReader(stdout)

	clickhouse := exec.Command("/usr/bin/clickhouse-client")
	clickhouse.Args = append(clickhouse.Args, "--query=INSERT INTO hlcup2018.accounts FORMAT CSV")

	ctdin, _ := clickhouse.StdinPipe()

	if err = loader.Start(); err != nil {
		log.Fatal(err)
	}

	if err = clickhouse.Start(); err != nil {
		log.Fatal(err)
	}

	var jcon, fcon, writef int

	for _, json := range buff {
		json = bytes.TrimSpace(json)
		//json = bytes.Replace(json, []byte{'\n'}, []byte(""), 9000) // is it necessary?

		_, err = stdin.Write(append(json, '\n'))
		if err != nil {
			log.Println(err)
			break
		}

		csv, _, err := br.ReadLine()
		if err != nil {
			log.Println(err)

			estr, err := ioutil.ReadAll(stderr)
			if err != nil {
				log.Println(err)
				break
			}

			fmt.Println(string(estr))
			fmt.Println(string(json))
			break
		}

		if bytes.Equal(csv, []byte("continue#json")) {
			jcon++
			continue
		}

		if bytes.Equal(csv, []byte("continue#format")) {
			fcon++
			continue
		}

		_, err = ctdin.Write(append(csv, '\n'))
		if err != nil {
			clickhouse = exec.Command("/usr/bin/clickhouse-client")
			clickhouse.Args = append(clickhouse.Args, "--query=INSERT INTO hlcup2018.accounts FORMAT CSV")

			ctdin, _ = clickhouse.StdinPipe()

			if err = clickhouse.Start(); err != nil {
				fmt.Println("-cl")
				log.Println(err)
				break
			}

			fmt.Println("+cl")

			_, err = ctdin.Write(append(csv, '\n'))
			if err != nil {
				writef++
				continue
			}
		}

		iters++
	}

	if err = stdin.Close(); err != nil {
		log.Fatal(err)
	}

	if err = ctdin.Close(); err != nil {
		log.Fatal(err)
	}

	loader.Wait()

	clickhouse.Wait()

	buff = buff[:0]

	atomic.AddUint32(&queueIters, 1)

	fmt.Printf("csv: %d, %d, %d\n", l, iters, queueIters) // lines, iters

	fmt.Printf("j,f,w: %d, %d, %d\n\n", jcon, fcon, writef)
}

func iterQueue() {
	var buff, backup [][]byte

	last := time.Now()

	for {
		select {
		case n := <-csvQueue:
			buff = append(buff, *(n.pb))
			(*n).ch <- true
			if len(buff) > 299 {
				backup = buff
				go pushQueue(&backup)
				buff = make([][]byte, 0)
				last = time.Now()
			}
		case <-csvTrigger:
			if len(buff) > 0 && last.Add(10*time.Second).Before(time.Now()) {
				backup = buff
				go pushQueue(&backup)
				buff = make([][]byte, 0)
				last = time.Now()
			}
		}
	}
}

func tickQueue() {
	for {
		time.Sleep(10 * time.Second)

		csvTrigger <- true
	}
}

func main() {
	//host := flag.String("host", "localhost", "an address to serve")
	port := flag.Int("port", 8080, "a port to listen")
	psql := flag.Bool("psql", false, "print sql requests")

	flag.Parse()

	pSQL = *psql

	csvQueue = make(chan *chanInc)
	csvTrigger = make(chan bool)

	go iterQueue()
	go tickQueue()

	pstr := fmt.Sprintf(":%d", *port)

	fmt.Printf("serving %s/\n\n", pstr)

	fasthttp.ListenAndServe(pstr, getRouter().HandleRequest)
}
