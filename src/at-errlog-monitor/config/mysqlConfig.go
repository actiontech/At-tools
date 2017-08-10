// configuration file example:
// [default]
// log-error-path-scan = 60

// [mysql-3306]
// user = root
// password = 123
// host =
// socket = /opt/mysql/data/3306/mysqld.sock
// connect-timeout = 10
// exec-sql-timeout = 10
package config

import (
	"at-errlog-monitor/conf"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

var (
	myConfig = newConfig()
	mutex    sync.RWMutex
)

type config struct {
	mysqls map[MysqlPort]MysqlConfig
}

func newConfig() *config {
	return &config{
		mysqls: map[MysqlPort]MysqlConfig{},
	}
}

type MysqlPort string

type MysqlConfig struct {
	MysqlUser           string
	MysqlPassword       string
	MysqlPort           MysqlPort
	MysqlHost           string
	MysqlSocket         string
	MysqlConnectTimeout int
	MysqlExecSqlTimeout int
	LogErrorScanSeconds int
}

func ApplyConfig(fnOnAddedConf, fnOnReloadConf, fnOnDeletedConf func(m MysqlConfig)) error {
	// TODO: assign config path
	newConf, err := verify("./config")
	if nil != err {
		return err
	}

	mutex.Lock()
	for k, v := range newConf.mysqls {
		if _, ok := myConfig.mysqls[k]; ok {
			fnOnReloadConf(v)
		} else {
			fnOnAddedConf(v)
		}
		delete(myConfig.mysqls, k)
	}

	for _, v := range myConfig.mysqls {
		fnOnDeletedConf(v)
	}

	myConfig = newConf
	mutex.Unlock()
	return nil
}

func GetMysqlConfig(port MysqlPort) (MysqlConfig, error) {
	mutex.RLock()
	defer mutex.RUnlock()
	if m, ok := myConfig.mysqls[port]; ok {
		return m, nil
	}
	return MysqlConfig{}, fmt.Errorf("no such mysql")
}

func verify(configPath string) (*config, error) {
	c := newConfig()
	cf, err := conf.ReadConfigFile(configPath)
	if nil != err {
		return nil, err
	}

	cErr := []string{}

	getStringWithErr := func(section, option string) string {
		value, err := cf.GetString(section, option)
		if nil != err {
			cErr = append(cErr, err.Error())
		}
		return value
	}

	getIntWithErr := func(section, option string) int {
		value, err := cf.GetInt(section, option)
		if nil != err {
			cErr = append(cErr, err.Error())
		}
		return value
	}

	scan, err := cf.GetInt("default", "log-error-path-scan")
	if nil != err {
		cErr = append(cErr, err.Error())
	}
	if scan <= 0 {
		cErr = append(cErr, fmt.Sprintf("log-error-path-scan should be positive integer!"))
	}

	for _, s := range cf.GetSections() {
		if strings.HasPrefix(s, "mysql-") {
			port := strings.TrimLeft(s, "mysql-")
			if _, err = strconv.Atoi(port); nil != err {
				cErr = append(cErr, s+" port should be an integer")
			}
			m := MysqlConfig{
				MysqlUser:           getStringWithErr(s, "user"),
				MysqlPassword:       getStringWithErr(s, "password"),
				MysqlPort:           MysqlPort(port),
				MysqlHost:           getStringWithErr(s, "host"),
				MysqlSocket:         getStringWithErr(s, "socket"),
				MysqlConnectTimeout: getIntWithErr(s, "connect-timeout"),
				MysqlExecSqlTimeout: getIntWithErr(s, "exec-sql-timeout"),
				LogErrorScanSeconds: scan,
			}
			c.mysqls[m.MysqlPort] = m
		}
	}

	if len(cErr) > 0 {
		return nil, fmt.Errorf("config error: %v", strings.Join(cErr, ".\n"))
	}
	return c, nil
}
