package mysql

import (
	"sync"
	"database/sql"
	"log"
	"fmt"
	"time"
	"os"
	_ "github.com/go-sql-driver/mysql"
)

var LOGGER = log.New(os.Stderr, "MySQL:", log.LstdFlags)

type DB struct {
	*sql.DB
	sqlExecutionTimeout int
}

type dbCacheKey struct {
	user           string
	password       string
	socket         string
	host           string
	port           string
	connectTimeout int
	execSqlTimeout int
}

var dbCache = make(map[dbCacheKey]*DB)
var DbCacheMutex = sync.Mutex{}

func OpenDb(user, pass, socket, host, port string, connectTimeout, execSqlTimeout int) (db *DB, err error) {
	DbCacheMutex.Lock()
	defer DbCacheMutex.Unlock()
	firstTry := true
retry:
	key := dbCacheKey{
		user:           user,
		password:       pass,
		socket:         socket,
		host:           host,
		port:           port,
		connectTimeout: connectTimeout,
		execSqlTimeout: execSqlTimeout,
	}

	if _, found := dbCache[key]; !found {
		for existKey := range dbCache {
			if key.socket == existKey.socket && key.host == existKey.host && key.port == existKey.port && key.user == existKey.user {
				dbCache[existKey].Close()
				delete(dbCache, existKey)
			}
		}

		if "" != socket {
			if db, err = OpenDbBySocketWithoutCache(user, pass, socket, connectTimeout, execSqlTimeout); nil != err {
				return nil, err
			}
		} else {
			if db, err = OpenDbWithoutCache(user, pass, host, port, connectTimeout, execSqlTimeout); nil != err {
				return nil, err
			}
		}
		db.SetMaxIdleConns(5)
		db.SetMaxOpenConns(20)
		dbCache[key] = db
	}
	db = dbCache[key]
	if err := pingDbOrTimeout(db.DB, 5 /* hard-coding */); nil != err {
		db.Close()
		delete(dbCache, key)
		if firstTry {
			firstTry = false
			goto retry
		} else {
			return nil, err
		}
	} else {
		return db, nil
	}
}

func OpenDbWithoutCache(user, pass, host, port string, connectTimeout, execSqlTimeout int) (db *DB, err error) {
	LOGGER.Printf("open db: host=%v; port=%v; connectTimeout=%v; execSqlTimeout=%v", host, port, connectTimeout, execSqlTimeout)
	return openDbByConnStrWithoutCache(fmt.Sprintf("%s:%s@(%s:%s)/?timeout=%ds", user, pass, host, port, connectTimeout), host+":"+port, execSqlTimeout)
}
func OpenDbBySocketWithoutCache(user, pass, socket string, connectTimeout, execSqlTimeout int) (db *DB, err error) {
	LOGGER.Printf("open db: socket=%v; connectTimeout=%v; execSqlTimeout=%v", socket, connectTimeout, execSqlTimeout)
	return openDbByConnStrWithoutCache(fmt.Sprintf("%s:%s@unix(%v)/?timeout=%ds", user, pass, socket, connectTimeout), socket, execSqlTimeout)
}

func openDbByConnStrWithoutCache(connectCmd, flag string, execHaSqlTimeout int) (db *DB, err error) {
	started := time.Now()
	defer func() {
		if delta := time.Now().Sub(started).Seconds(); delta > 5 {
			LOGGER.Printf("!!! open db %v exceed 5s (actual %vs)", flag, int(delta))
		}
	}()

	sqlDb, err := sql.Open("mysql", connectCmd)
	if nil != err {
		return nil, err
	}
	if err := pingDbOrTimeout(sqlDb, 5 /* hard-coding */); nil != err {
		sqlDb.Close()
		return nil, err
	}
	db = &DB{sqlDb, execHaSqlTimeout}
	return db, nil
}

func pingDbOrTimeout(db *sql.DB, timeoutSeconds int) error {
	var pingErr error
	finished := make(chan bool, 1)
	go func() {
		pingErr = Ping(db)
		finished <- true
	}()
	select {
	case <-finished:
		return pingErr
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		return fmt.Errorf("ping db timeout")
	}
}

func Ping(db *sql.DB) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Recovered in db.Ping():%v", r)
		}
	}()
	err = db.Ping()
	return err
}

func SqlQuery(db *DB, s string, arguments ...interface{}) ([]map[string]string, error) {
	result := []map[string]string{}
	mutex := sync.Mutex{} //TRY to fix mystery
	err := sqlQueryForEachRow(db, s, []string{}, func(row map[string]string, hasNonEmptyField bool) bool {
		mutex.Lock()
		result = append(result, row)
		mutex.Unlock()
		return true
	}, arguments...)
	if nil != err {
		return nil, err
	}
	mutex.Lock()
	defer mutex.Unlock()
	return result, nil
}

func sqlQueryForEachRow(db *DB, s string, columns []string, iter func(row map[string]string, hasNonEmptyField bool) bool, arguments ...interface{}) (err error) {
	LOGGER.Printf("query {" + s + "}")
	rowsChan := make(chan *sql.Rows, 1)
	errChan := make(chan error, 2)
	var rows *sql.Rows
	go func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in sql.query")
				return
			}
		}()

		rows, err := db.Query(s, arguments...)
		if nil != err {
			errChan <- err
			return
		}
		rowsChan <- rows
		return
	}()

	select {
	case err := <-errChan:
		return err
	case rows = <-rowsChan:
	case <-time.After(time.Duration(db.sqlExecutionTimeout) * time.Second):
		err := fmt.Errorf("timeout")
		LOGGER.Printf("query {" + s + "} timeout")
		return err
	}

	if nil != rows {
		defer rows.Close()
		cols, _ := rows.Columns()
		buf := make([]interface{}, len(cols))
		data := make([]sql.NullString, len(cols))
		for i, _ := range buf {
			buf[i] = &data[i]
		}

		for rows.Next() {
			rows.Scan(buf...)
			row := make(map[string]string)
			if len(columns) == 0 {
				for _, col := range cols {
					row[col] = ""
				}
			} else {
				for _, col := range columns {
					row[col] = ""
				}
			}
			hasNonEmptyField := false
			for k, col := range data {
				if _, found := row[cols[k]]; found {
					row[cols[k]] = col.String
					if hasNonEmptyField || len(col.String) > 0 {
						hasNonEmptyField = true
					}
				}
			}
			if !iter(row, hasNonEmptyField) {
				break
			}
		}
	}
	return
}
