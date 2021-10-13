package main

import (
	"fmt"
	//"html"
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
)

type query_struct struct {
	Exec  string
	Query string
}

type response_struct struct {
	Time time.Duration
	Data [][]string
}

func mr_txt2list() [][]string {
	f, _ := os.Open("../tmp/output.txt")
	scanner := bufio.NewScanner(f)
	var data [][]string
	for scanner.Scan() {
		line := scanner.Text()
		temp := strings.Split(line[strings.Index(line, "\t")+1:len(line)], ",")
		data = append(data, temp)
	}
	//fmt.Println(data)
	return data
}

func mapreduce(rw http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		panic(err)
	}
	log.Println(string(body))
	var q query_struct
	err = json.Unmarshal(body, &q)
	if err != nil {
		panic(err)
	}
	var res response_struct
	log.Println(q.Query)
	start := time.Now()
	cmd := exec.Command("/bin/bash", "../src/mapreduce/scripts/run.bash", q.Query)
	cmd.Output()
	elapsed := time.Since(start)
	res.Time = elapsed
	res.Data = mr_txt2list()
	rw.Header().Set("Content-Type", "application/json")
	rw.WriteHeader(http.StatusOK)
	json.NewEncoder(rw).Encode(res)
}

func spark(rw http.ResponseWriter, req *http.Request) {
	// body, _ := ioutil.ReadAll(req.Body)

	// var q query_struct
	// err = json.Unmarshal(body, &q)
}

func main() {
	http.HandleFunc("/mapreduce", mapreduce)

	http.HandleFunc("/hi", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hi")
	})

	log.Fatal(http.ListenAndServe(":5555", nil))
}
