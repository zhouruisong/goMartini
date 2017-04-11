package main

import (
	// "time"
	"./domainmgr"
	"./logger"
	"./sync"
	"./tair"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/go-martini/martini"
	"io/ioutil"
	"os"
//	"reflect"
	//	"time"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

type Config struct {
	LogPath         string   `json:"log_path"`  //各级别日志路径
	MysqlDsn        string   `json:"mysql_dsn"` //后台存储dsn
	MysqlIp         string   `json:"mysql_ip"`
	MysqlUserName   string   `json:"mysql_username"`
	MysqlPassword   string   `json:"mysql_password"`
	MsqlPort        uint16   `json:"mysql_port"`
	MysqlBackupList string   `json:"mysql_standby_list"`
	ListenPort      int      `json:"listen_port"` //监听端口号
	TrackerServer   []string `json:"tracker_server"`
	MinConnection   int      `json:"fdfs_min_connection_count"`
	MaxConnection   int      `json:"fdfs_max_connection_count"`
	TairClient      string   `json:"tair_client"`
	TairServer      []string `json:"tair_server"`
	ServerId        uint32   `json:"server_Id"`
	EachSyncNum     int      `json:"each_sync_num"`
	Dbconns         int      `json:"dbconns"`
	Dbidle          int      `json:"dbidle"`
}

func loadConfig(path string) *Config {
	if len(path) == 0 {
		panic("path of conifg is null.")
	}

	_, err := os.Stat(path)
	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	var cfg Config
	b, err := ioutil.ReadAll(f)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(b, &cfg)
	if err != nil {
		panic(err)
	}

	return &cfg
}

func main() {
	var cfg_path string
	flag.StringVar(&cfg_path, "conf", "../conf/conf.json", "config file path")
	flag.Parse()
	fmt.Println(cfg_path)

	cfg := loadConfig(cfg_path)

	l := logger.GetLogger(cfg.LogPath, "init")
	l.Infof("cluster backup start.")

	l.Infof("cluster backup start.%+v", cfg)

	d := logger.GetLogger(cfg.LogPath, "binlog")
	r := logger.GetLogger(cfg.LogPath, "domainmgr")
	c := logger.GetLogger(cfg.LogPath, "tair")
	s := logger.GetLogger(cfg.LogPath, "sync")

	pBinlog := domainmgr.NewBinLogMgr(cfg.MysqlIp, cfg.MsqlPort, cfg.MysqlUserName,
		cfg.MysqlPassword, cfg.ServerId, cfg.EachSyncNum, cfg.Dbconns, cfg.Dbidle, d)
	if pBinlog == nil {
		l.Errorf("NewBinLogMgr fail")
		return
	}

	pStreaminfo := domainmgr.NewStreamMgr(cfg.MysqlDsn, cfg.MysqlBackupList, r)
	if pStreaminfo == nil {
		l.Errorf("NewStreamMgr fail")
		return
	}

	pTair := tair.NewTairClient(cfg.TairServer, cfg.TairClient, c)
	if pTair == nil {
		l.Errorf("NewTairClient fail")
		return
	}

	pSync := sync.NewSyncMgr(pStreaminfo, pBinlog, pTair, nil, s)
	if pSync == nil {
		l.Errorf("NewSyncMgr fail")
		return
	}

	go pSync.IncreaseSync()
//	go pSync.TotalSync()

	m := martini.Classic()
	m.Run()
}
