package mysqlmgr

import (
	"../binlogmgr"
	"database/sql"
	log "github.com/Sirupsen/logrus"
)

var (
	db *sql.DB
	g_dbconns     = 200
	g_dbidleconns = 100
	g_dsn = ""
)

// return of sending mysql data to other
type MsgInsertRet struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

type DataDetail struct {
	info binlogmgr.DbInfo
}

// send mysql data to other
type MsgInsertBody struct {
	TableName string
	Data      DataDetail
}

type MysqlMgr struct {
	Logger      *log.Logger
	FdfsBackup string
}

func NewMysqlMgr(dsn string, dbconn int, dbidleconn int, fdfsip string, lg *log.Logger) *MysqlMgr {
	mgr := &MysqlMgr{
		Logger:          lg,
		FdfsBackup:      fdfsip,
	}
	
	g_dbconns     = dbconn
	g_dbidleconns = dbidleconn
	g_dsn = dsn
	
	err := mgr.init()
	if err != nil {
		mgr.Logger.Errorf("mgr.init failed")
		return nil
	}
	mgr.Logger.Infof("NewMysqlMgr ok")
	return mgr
}

func (mgr *MysqlMgr) init() error {
	sqldb, err := sql.Open("mysql", g_dsn)
	if err != nil {
		mgr.Logger.Errorf("err:%v.\n", err)
		return err
	}

	db = sqldb
	db.SetMaxOpenConns(g_dbconns)
	db.SetMaxIdleConns(g_dbidleconns)
	db.Ping()
	
	return nil
}