package main

import (
	"at-errlog-monitor/config"
	"at-errlog-monitor/mysql"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/hpcloud/tail"
)

var ( // TODO: make them configurable
	logger           = log.New(os.Stderr, "", log.LstdFlags)
	outPut io.Writer = os.Stdout // var _ io.Writer = (output)(nil) make sure output implements io.Writer
)

var observers []observer

func init() {
	observers = []observer{&simpleObserver{}} // TODO: define each observer, and make them configurable
}

type observer interface {
	outputHandler(identity config.MysqlPort, lineCh <-chan *tail.Line)
}

type control struct {
	configCh   chan config.MysqlConfig
	shutdownCh chan bool
}

func newControl(configCh chan config.MysqlConfig, shutdownCh chan bool) *control {
	return &control{
		configCh:   configCh,
		shutdownCh: shutdownCh,
	}
}

func main() {
	controls := make(map[config.MysqlPort]*control)
	s := make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGUSR2)
	for {
		s1 := <-s
		logger.Printf("Got signal: %v", s1)
		if err := config.ApplyConfig(
			//added
			func(m config.MysqlConfig) {
				logger.Printf("adding config: %v", m.MysqlPort)
				configCh := make(chan config.MysqlConfig)
				controls[m.MysqlPort] = newControl(configCh, startKeepErrLogPath(m.MysqlPort, configCh))
				configCh <- m
				logger.Printf("added config: %v", m.MysqlPort)
			},
			//reload
			func(m config.MysqlConfig) {
				logger.Printf("reloading config: %v", m.MysqlPort)
				controls[m.MysqlPort].configCh <- m
				logger.Printf("reloaded config: %v", m.MysqlPort)
			},
			//delete
			func(m config.MysqlConfig) {
				logger.Printf("deleting config: %v", m.MysqlPort)
				controls[m.MysqlPort].shutdownCh <- true
				delete(controls, m.MysqlPort)
				logger.Printf("deleted config: %v", m.MysqlPort)
			}); nil != err {
			logger.Printf("bad config: %v", err.Error())
		}
	}
}

func startKeepErrLogPath(identity config.MysqlPort, mysqlConfig chan config.MysqlConfig) (shutdownCh chan bool) {
	pathCh := make(chan string)
	shutdownCh = make(chan bool)
	go func() {
		var c config.MysqlConfig
		select {
		case c = <-mysqlConfig:
		}
		for {
			if path, err := getErrLogPath(c); nil != err {
				logger.Printf("ERROR: get log-error path error: %v", err)
			} else {
				pathCh <- path
				logger.Printf("get mysql log-error path (%v)", path)
			}

			select {
			case c = <-mysqlConfig:
			case <-time.Tick(time.Duration(c.LogErrorScanSeconds) * time.Second):
			case <-shutdownCh:
				close(pathCh)
				return
			}

		}
	}()
	startTailFile(identity, pathCh)
	return shutdownCh
}

func getErrLogPath(c config.MysqlConfig) (string, error) {
	db, err := mysql.OpenDb(c.MysqlUser, c.MysqlPassword, c.MysqlSocket, c.MysqlHost, string(c.MysqlPort), c.MysqlConnectTimeout, c.MysqlExecSqlTimeout)
	if nil != err {
		return "", err
	}
	data, err := mysql.SqlQuery(db, "select @@datadir")
	if nil != err {
		return "", err
	}
	errLog, err := mysql.SqlQuery(db, "select @@log_error")
	if nil != err {
		return "", err
	}
	return filepath.Join(data[0]["@@datadir"], errLog[0]["@@log_error"]), nil
}

func showProcessList(c config.MysqlConfig) (string, error) {
	db, err := mysql.OpenDb(c.MysqlUser, c.MysqlPassword, c.MysqlSocket, c.MysqlHost, string(c.MysqlPort), c.MysqlConnectTimeout, c.MysqlExecSqlTimeout)
	if nil != err {
		return "", err
	}
	data, err := mysql.SqlQuery(db, "show processlist")
	if nil != err {
		return "", err
	}
	return fmt.Sprintf("%v", data), nil
}

func startTailFile(identity config.MysqlPort, pathCh <-chan string) {
	go func() {
		var t *tail.Tail
		for {
			var err error
			select {
			case path, ok := <-pathCh:
				if !ok {
					return
				}
				if nil != t && path == t.Filename {
					continue
				}
				if nil != t {
					err = t.Stop()
					logger.Printf("stop tail file(%v) reason: %v", t.Filename, err)
					t = nil
				}
				if "" != path {
					t, err = tail.TailFile(path, tail.Config{Follow: true, ReOpen: true})
					if nil != err {
						logger.Printf("start tail file(%v) error: %v", t.Filename, err)
					}
					for _, obs := range observers {
						obs.outputHandler(identity, t.Lines)
					}
				}
			}
		}
	}()
}

type simpleObserver struct { // for demo
}

func (s *simpleObserver) outputHandler(identity config.MysqlPort, lineCh <-chan *tail.Line) {
	go func() {
		for l := range lineCh {
			if strings.Contains(l.Text, "[Warning]") {
				fmt.Fprintf(outPut, "%v : %v\n", identity, l.Text)
				if m, err := config.GetMysqlConfig(identity); nil != err {
					fmt.Fprintf(outPut, "ERROR: "+err.Error())
				} else if data, err := showProcessList(m); nil != err {
					fmt.Fprintf(outPut, "ERROR: "+err.Error())
				} else {
					fmt.Fprintf(outPut, "show processlist: "+data+"\n")
				}
			}
		}
	}()
}
