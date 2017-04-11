package binlogmgr

import (
	"fmt"
	"os"
	"strings"
	"time"
	//	"reflect"
	"database/sql"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

var (
	host             = "localhost"
	port             uint16
	username         = "root"
	password         = "123456"
	server_Id        uint32
	eachNum          = 100
	dbconns          = 200
	dbidleconns      = 100
	binlogfile       = ""
	g_sync_part_time = false
	db               *sql.DB
	g_fd             *os.File
)

// mysql db of one row
type DbInfo struct {
	TaskId           string
	FileName         string
	FileType         string
	FileSize         int32
	Domain           string
	Status           int32
	Action           string
	Md5Type          int16
	DnameMd5         string
	SourceUrl        string
	TransCodingUrl   string
	FileMd5          string
	IndexMd5         string
	HeadMd5          string
	ExpiryTime       string
	CreateTime       string
	ExecTime         string
	CbUrl            string
	FfUri            string
	TaskBranchStatus string
	LocalServerDir   string
	TsUrl            string
	Type             int8
	TransCodingInfo  string
	IsBackup         int8
}

type TimeInfo struct {
	hour   int
	minute int
	second int
}

type BinLogMgr struct {
	Logger     *log.Logger
	EventChan  chan *DbInfo
	tablename  string
	start_time TimeInfo
	end_time   TimeInfo
}

func NewBinLogMgr(host_ string, port_ uint16, username_ string,
	password_ string, serverId_ uint32, each int, dbconn int,
	dbidle int, binlogfile_ string, starttime string, endtime string,
	lg *log.Logger) *BinLogMgr {

	my := &BinLogMgr{
		Logger:    lg,
		EventChan: make(chan *DbInfo, 10000),
		tablename: "",
		start_time: TimeInfo{
			hour:   0,
			minute: 0,
			second: 0,
		},
		end_time: TimeInfo{
			hour:   23,
			minute: 59,
			second: 0,
		},
	}

	host = host_
	port = port_
	username = username_
	password = password_
	server_Id = serverId_
	eachNum = each
	dbconns = dbconn
	dbidleconns = dbidle
	binlogfile = binlogfile_

	rt := GetSyncTime(my, starttime, endtime)
	if rt != true {
		my.Logger.Errorf("GetSyncTime failed")
		return nil
	}

	var err error
	g_fd, err = os.OpenFile(binlogfile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		my.Logger.Errorf("OpenFile failed")
		return nil
	}

	my.Logger.Infof("sync starttime:%+v, sync endtime:%+v", my.start_time, my.end_time)
	my.Logger.Infof("g_sync_part_time:%+v", g_sync_part_time)
	my.Logger.Infof("NewBinLogMgr ok")
	return my
}

//打印内容到文件中
func (mgr *BinLogMgr) tracefile(str_content string) {
	fd_content := strings.Join([]string{str_content, "\n"}, "")
	buf := []byte(fd_content)
	g_fd.Write(buf)
}

func (mgr *BinLogMgr) Read() *DbInfo {
	info := <-mgr.EventChan
	return info
}

func (mgr *BinLogMgr) Write(info *DbInfo) int {
	select {
	case mgr.EventChan <- info:
		return 0
	case <-time.After(time.Second * 2):
		mgr.Logger.Infof("write to channel timeout: %+v", info)
		return -1
	}
	return 0
}

func (mgr *BinLogMgr) StartPartTimeSync() {
	if g_sync_part_time && CheckStartEndTime(mgr) {
	}
}

func (mgr *BinLogMgr) RunMoniterMysql() {
	mgr.Logger.Infof("start run RunMoniterMysql")
	cfgsql := replication.BinlogSyncerConfig{
		ServerID: server_Id,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
	}

	syncer := replication.NewBinlogSyncer(&cfgsql)

	url := fmt.Sprintf("%s:%d", host, port)
	c, err := client.Connect(url, username, password, "live_master")
	if err != nil {
		panic(err.Error())
	}

	r, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		panic(err.Error())
	}

	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	mgr.Logger.Infof("binFile:%+v, binPos:%+v", binFile, binPos)

	// Start sync with sepcified binlog file and position
	streamer, _ := syncer.StartSync(mysql.Position{binFile, (uint32)(binPos)})

	for {
		ev, _ := streamer.GetEvent(context.Background())
		var r replication.Event
		r = ev.Event

		//		fmt.Printf("type: %+v", reflect.TypeOf(r))
		//		ev.Dump(os.Stdout)

		//		if ev, ok := r.(*replication.TableMapEvent); ok {
		//			mgr.tablename = fmt.Sprintf("%s", ev.Table)
		//		}

		if ev, ok := r.(*replication.RowsEvent); ok {
			mgr.GetDump(ev, mgr.tablename)
		}
	}
}

func (mgr *BinLogMgr) GetDump(ev *replication.RowsEvent, tablename string) {
	var data DbInfo
	for _, rows := range ev.Rows {
		for k, d := range rows {
			if _, ok := d.([]byte); ok {
				//				mgr.Logger.Infof("type: %+v", reflect.TypeOf(d))
			} else {
				GetValue(&data, k, d)
			}
		}
	}

	mgr.Logger.Infof("data: %+v\n", data)

	//data.Status == 200 表示是源数据处理完毕，需要同步， 为1表示是备份数据，不需要同步
	if data.IsBackup == 0 && data.Status == 200 {
		// 指定时间同步，需要先写文件
		if g_sync_part_time {
			mgr.tracefile(fmt.Sprintf("http://%s/%s/%s", "127.0.0.1", data.Domain, data.FileName))
		} else {
			if mgr.Write(&data) != 0 {
				mgr.Logger.Errorf("write to channel fail: %+v\n", data)
			} else {
				mgr.Logger.Infof("write to channel successful: %+v\n", data)
			}
		}
	}
}
