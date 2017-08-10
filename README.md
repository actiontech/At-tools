# At-tools  

## at-errlog-monitor
跟踪MySQL的error log, 监控其中Warning信息, 并输出即时的show processlist结果值.   

### 使用  
1. 下载执行文件at-errlog-monitor  
2. 创建与at-errlog-monitor同目录的配置文件config 

```
[root@a8ce7ee2dacc test]# ls
at-errlog-monitor  config 
```


配置文件格式:  
```
[default]
log-error-path-scan = 60

[mysql-3306]
user = root
password = 123
host =
socket = /opt/mysql/data/3306/mysqld.sock
connect-timeout = 10
exec-sql-timeout = 10

[mysql-3309]
user = root
password = 123
host =
socket = /opt/mysql/data/3309/mysqld.sock
connect-timeout = 10
exec-sql-timeout = 10
```

3.运行执行文件 
```
[root@a8ce7ee2dacc test]# ./at-errlog-monitor 1> info 2> log &
[1] 81980
```

4.使用SIGUSR2加载配置  
```
[root@a8ce7ee2dacc test]# kill -SIGUSR2 81980
```
查看info可见warning及相关processlist  

### TODO  
* 忽略errlog中已存在的warning信息, 避免无用输出  
* 定义日志信息及回收  
