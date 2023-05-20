package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/ini.v1"
)

func LogSetting(setting string, logfile string) {
	if setting == "console" {
		mw := io.MultiWriter(os.Stdout)
		log.SetOutput(mw)
	}
	if setting == "file" {
		logFile, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		mw := io.MultiWriter(logFile)
		log.SetOutput(mw)
	}
	if setting == "both" {
		logFile, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		mw := io.MultiWriter(os.Stdout, logFile)
		log.SetOutput(mw)
	}
}

func ReadQuery(reader int, read_query string, read_loop_count int) {
	defer timer("Read_Query", reader, read_loop_count)()
	rows, err := db.Query(read_query)
	if err != nil {
		fmt.Errorf("read_query %v", err)
	}
	defer rows.Close()
	var rcount int64 = 0
	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		rcount++
		if err := rows.Err(); err != nil {
			fmt.Errorf("rowerror %v", err)
		}
	}
}

func ReadWrapper(reader int, read_query string, read_loop_count int, wg *sync.WaitGroup) {
	for i := 1; i <= read_loop_count; i++ {
		ReadQuery(reader, read_query, i)
	}
	wg.Done()
}

func WriteQuery(write_query string, wg *sync.WaitGroup) {
	defer timer("Write_Query", 1, 1)()
	rows, err := db.Query(write_query)
	if err != nil {
		fmt.Errorf("write_query %v", err)
	}
	defer rows.Close()

	var rcount int64 = 0
	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		rcount++
		if err := rows.Err(); err != nil {
			fmt.Errorf("rowerror %v", err)
		}

	}
	wg.Done()
}

func timer(name string, reader int, i int) func() {
	start := time.Now()
	return func() {
		log.Printf("Reader %v: %s run # %v took %v\n", reader, name, i, time.Since(start))
	}
}

var db *sql.DB

func main() {
	var err error
	cfg, err := ini.Load("settings.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}

	username := cfg.Section("").Key("username").String()
	password := cfg.Section("").Key("password").String()
	host := cfg.Section("").Key("host").String()
	db_name := cfg.Section("").Key("db_name").String()
	read_query := cfg.Section("").Key("read_query").String()
	write_query := cfg.Section("").Key("write_query").String()
	readers := cfg.Section("").Key("readers").MustInt()
	read_loop_count := cfg.Section("").Key("read_loop_count").MustInt()
	logging := cfg.Section("").Key("logging").String()
	logfile := cfg.Section("").Key("logfile").String()

	LogSetting(logging, logfile)

	dbcfg := mysql.Config{
		User:   username,
		Passwd: password,
		Net:    "tcp",
		Addr:   host,
		DBName: db_name,
	}

	db, err = sql.Open("mysql", dbcfg.FormatDSN())
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go ReadWrapper(i, read_query, read_loop_count, &wg)
	}
	wg.Add(1)
	go WriteQuery(write_query, &wg)

	wg.Wait()
}
