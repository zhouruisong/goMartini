package domainmgr

import (
	"fmt"
	// "strings"
	"database/sql"
	"time"
//	"os"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
	"strconv"
//	"reflect"
)

var (
	host        = "localhost"
	port  uint16
	username    = "root"
	password    = "123456"
	server_Id uint32
	eachNum     = 100
	dbconns     = 200
	dbidleconns = 100
	db          *sql.DB
)

type CheckInfo struct {
	Tablename string
	Info      StreamInfo
}

type InsertInfo struct {
	TaskId       string
	TaskServer   string
	FileName     string
	FileType     int8
	FileSize     int32
	FileMd5      string
	Domain       string
	App          string
	Stream       string
	Step         int8
	PublishTime  int64
	NotifyUrl    string
	NotifyReturn string
	Status       int8
	ExpireTime   string
	CreateTime   string
	UpdateTime   string
	EndTime      string
	NotifyTime   string
}

type StreamInfo struct {
	Id           int32
	TaskId       string
	TaskServer   string
	FileName     string
	FileType     int8
	FileSize     int32
	FileMd5      string
	Domain       string
	App          string
	Stream       string
	Step         int8
	PublishTime  int64
	NotifyUrl    string
	NotifyReturn string
	Status       int8
	ExpireTime   string
	CreateTime   string
	UpdateTime   string
	EndTime      string
	NotifyTime   string
}

type BinLogMgr struct {
	Logger    *log.Logger
	EventChan chan *CheckInfo
	tablename string
}

type StreamMgr struct {
	Logger          *log.Logger
	Dsn             string
	MysqlBackupList string
	StreamInfos     []StreamInfo
}

func NewBinLogMgr(host_ string, port_ uint16, username_ string, password_ string,
	serverId_ uint32, each int, dbconns int, dbidle int, lg *log.Logger) *BinLogMgr {

	my := &BinLogMgr{
		Logger:    lg,
		EventChan: make(chan *CheckInfo, 10000),
	}
	my.setParamter(host_, port_, username_, password_, serverId_, each, dbconns, dbidle)
	my.Logger.Infof("NewBinLogMgr ok")
	return my
}

func (mgr *BinLogMgr) setParamter(host_ string, port_ uint16, username_ string,
	password_ string, serverId_ uint32, each int, dbconn int, dbidle int) {

	host = host_
	port = port_
	username = username_
	password = password_
	server_Id = serverId_
	eachNum = each
	dbconns = dbconn
	dbidleconns = dbidle
}

func (mgr *BinLogMgr) Read() *CheckInfo {
	info := <-mgr.EventChan
	// mgr.Logger.Infof("Read successful: %+v", info)
	return info
}

func (mgr *BinLogMgr) Write(info *CheckInfo) int {
	select {
	case mgr.EventChan <- info:
		// mgr.Logger.Infof("write to channel successful: %+v", info)
		return 0
	case <-time.After(time.Second * 2):
		mgr.Logger.Infof("write to channel timeout: %+v", info)
		return -1
	}
	return 0
}

func (mgr *BinLogMgr) Reflectint32(ty interface{}) (int32, bool) {
	if value, ok := ty.(int32); ok {
		return value, true
	}

	return 0, false
}

func (mgr *BinLogMgr) Reflectint8(ty interface{}) (int8, bool) {
	if value, ok := ty.(int8); ok {
		return value, true
	}

	return 0, false
}

func (mgr *BinLogMgr) Reflectint64(ty interface{}) (int64, bool) {
	if value, ok := ty.(int64); ok {
		return value, true
	}

	return 0, false
}

func (mgr *BinLogMgr) ReflectString(ty interface{}) (string, bool) {
	if value, ok := ty.(string); ok {
		return value, true
	}

	return "", false
}

func (mgr *BinLogMgr) ReflectTime(ty interface{}) (string, bool) {
	if value, ok := ty.(time.Time); ok {
		return value.Format("2006-01-02 15:04:05"), true
	}

	return "", false
}

func (mgr *BinLogMgr) GetDump(ev *replication.RowsEvent, tablename string) {
	var data CheckInfo
	data.Info.FileType = 1
	
	for _, rows := range ev.Rows {
		for k, d := range rows {
			if _, ok := d.([]byte); ok {
//				value := fmt.Sprintf("%q", d)
//				mgr.Logger.Infof("type: %+v", reflect.TypeOf(d))
//				m = append(m, value)
			} else {
				switch k {
					case 0:
						data.Info.Id, _ = mgr.Reflectint32(d)
					case 1:
						data.Info.TaskId, _ = mgr.ReflectString(d)
					case 2:
						data.Info.TaskServer, _ = mgr.ReflectString(d)
					case 3:
						data.Info.FileName, _ = mgr.ReflectString(d)
					case 4:
						data.Info.FileType, _ = mgr.Reflectint8(d)
					case 5:
						data.Info.FileSize, _ = mgr.Reflectint32(d)
					case 6:
						data.Info.FileMd5, _ = mgr.ReflectString(d)
					case 7:
						data.Info.Domain, _ = mgr.ReflectString(d)
					case 8:
						data.Info.App, _ = mgr.ReflectString(d)
					case 9:
						data.Info.Stream, _ = mgr.ReflectString(d)
					case 10:
						data.Info.Step, _ = mgr.Reflectint8(d)
					case 11:
						data.Info.PublishTime, _ = mgr.Reflectint64(d)
					case 12:
						data.Info.NotifyUrl, _ = mgr.ReflectString(d)
					case 13:
						data.Info.NotifyReturn, _ = mgr.ReflectString(d)
					case 14:
						data.Info.Status, _ = mgr.Reflectint8(d)
					case 15:
						data.Info.ExpireTime, _ = mgr.ReflectString(d)
					case 16:
						data.Info.CreateTime, _ = mgr.ReflectString(d)
					case 17:
						data.Info.UpdateTime, _ = mgr.ReflectString(d)
					case 18:
						data.Info.EndTime, _ = mgr.ReflectString(d)
					case 19:
						data.Info.NotifyTime, _ = mgr.ReflectString(d)
					default:
						mgr.Logger.Errorf("wrong k: %+v", k)
				}
			}
		}
	}

	// data.Info.FileType == 0 表示是源数据，需要同步， 为1表示是备份数据，不需要同步
	if data.Info.FileType == 0 {
		data.Tablename = tablename
		if mgr.Write(&data) != 0 {
			mgr.Logger.Errorf("write to channel fail: %+v\n", data)
		} else {
			mgr.Logger.Infof("write to channel successful: %+v\n", data)
		}
	}
}
	
func (mgr *BinLogMgr) RunMoniterMysql() {	
	mgr.Logger.Infof("start run RunMoniterMysql")
	cfgsql := replication.BinlogSyncerConfig {
		ServerID: server_Id,
		Flavor:   "mysql",
		Host:     host,
		Port:     port,
		User:     username,
		Password: password,
	}
	syncer := replication.NewBinlogSyncer(&cfgsql)

	c, err := client.Connect(fmt.Sprintf("%s:%d", host, port), username, password, "live_master")
	if err != nil {
		panic(err.Error())
	}

	r, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		panic(err.Error())
	}

	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	// Start sync with sepcified binlog file and position
	streamer, _ := syncer.StartSync(mysql.Position{binFile, (uint32)(binPos)})
	
	for {
		ev, _ := streamer.GetEvent(context.Background())
		var r replication.Event
		r = ev.Event
		
//		fmt.Printf("type: %+v", reflect.TypeOf(r))
//		r.Dump(os.Stdout)

		if ev, ok := r.(*replication.TableMapEvent); ok {
			mgr.tablename = fmt.Sprintf("%s", ev.Table)
		}
		
		if ev, ok := r.(*replication.RowsEvent); ok {
			mgr.GetDump(ev, mgr.tablename)
		}
	}
}

func NewStreamMgr(dsn string, iplist string, lg *log.Logger) *StreamMgr {
	mgr := &StreamMgr{
		Logger:          lg,
		Dsn:             dsn,
		MysqlBackupList: iplist,
	}

	err := mgr.init()
	if err != nil {
		mgr.Logger.Infof("mgr.init failed")
		return nil
	}
	mgr.Logger.Infof("NewStreamMgr ok")
	return mgr
}

func (mgr *StreamMgr) init() error {
	var err error
	db, err = sql.Open("mysql", mgr.Dsn)
	if err != nil {
		mgr.Logger.Errorf("err:%v.\n", err)
		return err
	}

	db.SetMaxOpenConns(dbconns)
	db.SetMaxIdleConns(dbidleconns)
	db.Ping()
	return nil
}

func (mgr *StreamMgr) SelectTableName() ([]string, error) {
	var tablename []string

	querysql := "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'live_master'"
	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return tablename, err
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return tablename, err
	}

	var table string
	for rows.Next() {
		err := rows.Scan(&table)
		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return tablename, err
		}
		tablename = append(tablename, table)
	}

	mgr.Logger.Infof("tablename:%v, ", tablename)
	return tablename, nil
}

func (mgr *StreamMgr) SelectDataExist(taskId, tablename string) int {
	count := 0
	querysql := fmt.Sprintf("select count(1) from live_master.%s where %s.task_id = \"%s\" ",
		tablename, tablename, taskId)

	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return 1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return 1
	}

	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return 1
		}
	}

	mgr.Logger.Infof("taskId:%v, count: %v", taskId, count)
	return count
}

//从数据库中加载所有注册的设备信息
func (mgr *StreamMgr) LoadStreamInfos(beginIndex int, tablename string) ([]StreamInfo, int) {
	mgr.Logger.Infof("start LoadStreamInfos")
	tmpNum := beginIndex * eachNum

	var returnInfo []StreamInfo
	querysql := "select id,task_id,task_server,file_name,file_type,file_size,file_md5,domain,app,stream,step," +
		"publish_time,notify_url,notify_return,status,expiry_time,create_time,update_time,end_time,notify_time from live_master." +
		tablename + " limit " + strconv.Itoa(tmpNum) + "," + strconv.Itoa(eachNum)

	// only loda upload complete
	rows, err := db.Query(querysql)
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return returnInfo, -1
	}
	defer rows.Close()

	err = rows.Err()
	if err != nil {
		mgr.Logger.Errorf("err:%v", err)
		return returnInfo, -1
	}

	var id int32
	var taskId string
	var taskServer string
	var fileName string
	var fileType int8
	var fileSize int32
	var fileMd5 string
	var domain string
	var app string
	var stream string
	var step int8
	var publishTime int64
	var notifyUrl string
	var notifyReturn string
	var status int8
	var expireTime string
	var createTime string
	var updateTime string
	var endTime string
	var notifyTime string

	for rows.Next() {
		err := rows.Scan(&id, &taskId, &taskServer, &fileName, &fileType, &fileSize, &fileMd5,
			&domain, &app, &stream, &step, &publishTime, &notifyUrl, &notifyReturn, &status,
			&expireTime, &createTime, &updateTime, &endTime, &notifyTime)

		if err != nil {
			mgr.Logger.Errorf("err:%v", err)
			return returnInfo, -1
		}

		info := StreamInfo{
			Id:           id,
			TaskId:       taskId,
			TaskServer:   taskServer,
			FileName:     fileName,
			FileType:     fileType,
			FileSize:     fileSize,
			FileMd5:      fileMd5,
			Domain:       domain,
			App:          app,
			Stream:       stream,
			Step:         step,
			PublishTime:  publishTime,
			NotifyUrl:    notifyUrl,
			NotifyReturn: notifyReturn,
			Status:       status,
			ExpireTime:   expireTime,
			CreateTime:   createTime,
			UpdateTime:   updateTime,
			EndTime:      endTime,
			NotifyTime:   notifyTime,
		}
		returnInfo = append(returnInfo, info)
	}

	mgr.Logger.Infof("LoadStreamInfos len: %+v", len(returnInfo))
	if len(returnInfo) == 0 {
		return returnInfo, -1
	}

	return returnInfo, 0
}

//向数据库注册新设备信息
func (mgr *StreamMgr) InsertStreamInfos(i int) int {
	mgr.Logger.Infof("start InsertStreamInfos")
	insertsql := "INSERT INTO t_live2odv2_kuwo" +
		"(task_id,task_server,file_name,file_type,file_size,file_md5,domain,app,stream,step," +
		"publish_time,notify_url,notify_return,status,expiry_time,create_time,update_time,end_time,notify_time) " +
		"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	stmtIns, err := db.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}
	defer stmtIns.Close()

	_, err = stmtIns.Exec("8e9addd82febf91d0fffead1760bzho"+strconv.Itoa(i), "zhouruisong", "/voicelive/219705672_preprocess-1477649359414.m3u8", 0, 0, "", "push.xycdn.kuwo.cn", "voicelive", "219705672_preprocess", 3, 1477649359414, "http://127.0.0.1:8080/accept_test.php", "string(279) \"{\"task_id\":\"8e9addd82febf91d0fffead1760b507a\",\"domain\":\"push.xycdn.kuwo.cn\",\"app\":\"voicelive\",\"stream\":\"219705672_preprocess\",\"tag\":\"/voicelive/219705672_preprocess-1477649359414.m3u8\",\"vod_url\":\"test.com\",\"vod_md5\":\"\",\"vod_size\":\"0\",\"vod_star", 1, "0000-00-00 00:00:00", "2016-10-28 10:09:19", "0000-00-00 00:00:00", "0000-00-00 00:00:00", "2016-10-28 10:09:29")
	if err != nil {
		mgr.Logger.Errorf("insert into mysql failed, err:%v", err)
		return -1
	}

	mgr.Logger.Infof("insert into mysql ok")
	return 0
}

//向数据库注册新设备信息
func (mgr *StreamMgr) InsertMultiStreamInfos(info []StreamInfo, tablename string) int {
	datalen := len(info)
	if datalen == 0 {
		mgr.Logger.Errorf("datalen = 0")
		return -1
	}

	insertsql := "INSERT INTO live_master." + tablename + " (task_id,task_server,file_name,file_type,file_size,file_md5,domain,app,stream,step," +
		"publish_time,notify_url,notify_return,status,expiry_time,create_time,update_time,end_time,notify_time) " +
		"VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

	start := time.Now()
	//Begin函数内部会去获取连接
	tx, err := db.Begin()
	if err != nil {
		mgr.Logger.Errorf("db.Begin(), err:%v", err)
		return -1
	}

	stmtIns, err := tx.Prepare(insertsql)
	if err != nil {
		mgr.Logger.Errorf("Prepare failed, err:%v", err)
		return -1
	}
	defer stmtIns.Close()

	for i := 0; i < datalen; i++ {
		count := mgr.SelectDataExist(info[i].TaskId, tablename)
		if count != 0 {
			mgr.Logger.Errorf("taskid:%v exist in %v", info[i].TaskId, tablename)
			continue
		}

		//每次循环用的都是tx内部的连接，没有新建连接，效率高
		stmtIns.Exec(info[i].TaskId, info[i].TaskServer, info[i].FileName, info[i].FileType, info[i].FileSize,
			info[i].FileMd5, info[i].Domain, info[i].App, info[i].Stream, info[i].Step, info[i].PublishTime, info[i].NotifyUrl,
			info[i].NotifyReturn, info[i].Status, info[i].ExpireTime, info[i].CreateTime, info[i].UpdateTime, info[i].EndTime, info[i].NotifyTime)
	}
	//出异常回滚
	defer tx.Rollback()

	//最后释放tx内部的连接
	tx.Commit()

	end := time.Now()
	mgr.Logger.Infof("insert ok total time: %v", end.Sub(start).Seconds())

	return 0
}
