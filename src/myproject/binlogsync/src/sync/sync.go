package sync

import (
	"../binlogmgr"
	"../indexmgr"
	"../mysqlmgr"
	"../protocal"
	"../transfer"
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var isOpen = 0

type SyncMgr struct {
	pMy          *binlogmgr.BinLogMgr
	pCl          *transfer.TransferMgr
	pSql         *mysqlmgr.MysqlMgr
	Logger       *log.Logger
	UploadServer string
}

func NewSyncMgr(server string, my *binlogmgr.BinLogMgr,
	cl *transfer.TransferMgr, sql *mysqlmgr.MysqlMgr, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pMy:          my,
		pCl:          cl,
		pSql:         sql,
		Logger:       lg,
		UploadServer: server,
	}

	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sync *SyncMgr) SetFlag(flag int) {
	isOpen = flag
}

func (sync *SyncMgr) SendUploadServer(data *protocal.DbInfo) (int, error) {
	msg := protocal.UploadInfo{
		TaskId:     data.TaskId,
		Domain:     data.Domain,
		FileName:   data.FileName,
		FileType:   data.FileType,
		SourceUrl:  data.SourceUrl,
		CbUrl:      data.CbUrl,
		Behavior:   "UP",
		CreateTime: data.CreateTime,
	}

	buff, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed err:%v, msg:%+v", err, msg)
		return -1, err
	}

	url := fmt.Sprintf("http://%v/index.php?Action=LiveMaintain.FileTaskAddNew", sync.UploadServer)
	ip := strings.Split(sync.UploadServer, ":")
	hosturl := fmt.Sprintf("application/json;charset=utf-8;hostname:%v", ip[0])

	body := bytes.NewBuffer([]byte(buff))
	res, err := http.Post(url, hosturl, body)
	if err != nil {
		sync.Logger.Errorf("http post return failed.err:%v , buff:%+v", err, string(buff))
		return -1, err
	}

	defer res.Body.Close()

	result, err := ioutil.ReadAll(res.Body)
	if err != nil {
		sync.Logger.Errorf("ioutil readall failed, err:%v, buff:%+v", err, string(buff))
		return -1, err
	}

	var ret protocal.RetUploadMeg
	err = json.Unmarshal(result, &ret)
	if err != nil {
		sync.Logger.Errorf("Unmarshal return body error, err:%v, buff:%+v", err, string(buff))
		return -1, err
	}

	sync.Logger.Infof("ret: %+v", ret)

	if ret.Code == 0 {
		return 0, nil
	}

	return -1, nil
}

func (sync *SyncMgr) sendFileToBackupFdfs(pMap *protocal.IndexMap, data *protocal.DbEventInfo) int {
	// send slice buff
	for _, item := range pMap.Item {
		// get data to master fdfs
		r, buf := sync.getFileFromFdfs(item.Id)
		if r == 0 {
			// put data to standby fdfs
			id := sync.pCl.Sendbuff(buf, data.DbData.TaskId)
			sync.Logger.Infof("id: %+v", id)
			if id != "" {
				// put data id to standby tair
				ret := sync.putIndexFile(data, id)
				sync.Logger.Infof("ret: %+v", ret)
				if ret != 0 {
					// delete data of standby fdfs use id
					rt := sync.pCl.Deletebuff(id)
					sync.Logger.Infof("rt: %+v", rt)
					if rt != 0 {
						sync.Logger.Errorf("delete data from standby fdfs failed. TaskId:%+v, id:%+v",
							data.DbData.TaskId, id)
						return -1
					}
				}
				sync.Logger.Infof("id:%+v", id)
				item.Id = id
			} else {
				sync.Logger.Errorf("put data to standby fdfs failed. TaskId: %+v",
					data.DbData.TaskId)
				//return -1
			}
		} else {
			sync.Logger.Errorf(" get data to master fdfs failed. TaskId: %+v", data.DbData.TaskId)
			return -1
		}
	}

	sync.Logger.Infof("send slice buff data to standby fdfs successful, taskId: %+v", data.DbData.TaskId)

	//二级索引的所有文件已经转移完毕，请将二级索引文件上传到fastdfs并且存储id到tair
	var total string
	for _, item := range pMap.Item {
		line := fmt.Sprintf("%s %s %s %s %s\n", item.Name, item.Id, item.Status, item.Size, item.Md5)
		total = total + line
	}

	sync.Logger.Infof("index total:\n%+v", total)

	buf := []byte(total)
	// put index data to standby fdfs
	id := sync.pCl.Sendbuff(buf, data.DbData.TaskId)
	if id == "" {
		sync.Logger.Errorf("put index data to standby fdfs failed. TaskId: %+v", data.DbData.TaskId)
		return -1
	}

	sync.Logger.Infof("put index data to standby fdfs successful, taskId: %+v", data.DbData.TaskId)

	// put index id to standby tair
	ret := sync.putIndexFile(data, id)
	if ret != 0 {
		// delete data of standby fdfs use id
		rt := sync.pCl.Deletebuff(id)
		if rt != 0 {
			sync.Logger.Errorf("delete data from standby fdfs failed. TaskId:%+v, id:%+v",
				data.DbData.TaskId, id)
		}
		sync.Logger.Errorf("put index id to standby tair failed. TaskId: %+v", data.DbData.TaskId)
		return -1
	}

	sync.Logger.Infof("put index id to standby tair successful, taskId: %+v", data.DbData.TaskId)

	// send db data to standby mysql
	rt := sync.pSql.SendMysqlbuff(&data.DbData, data.TableName)
	if rt != 0 {
		sync.Logger.Errorf("send db data to standby mysql failed. TaskId: %+v", data.DbData.TaskId)
		return -1
	}

	sync.Logger.Infof("sync db data successful, taskId: %+v", data.DbData.TaskId)

	return 0
}

func (sync *SyncMgr) getFileFromFdfs(id string) (int, []byte) {
	var ret []byte
	// get file from fdfs
	rt, fileBuff := sync.pCl.PFdfs.HandlerDownloadFile(id)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ret
	}

	//sync.Logger.Infof("fileBuff: %+v", string(fileBuff))

	//	filename := pData.TaskId + pData.FileName
	//	file, _ := os.Create(filename)
	//	defer file.Close()
	//	file.Write(fileBuff)

	return 0, fileBuff
}

func (sync *SyncMgr) putIndexFile(data *protocal.DbEventInfo, id string) int {
	//	creat_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.CreateTime)
	//	creat_time_u := creat_time.Unix()
	//
	//	expiry_time, _ := time.Parse("2006-01-02 15:04:05", data.DbData.ExpiryTime)
	//	expiry_time_u := expiry_time.Unix()

	keys := protocal.SendTairPut{
		Prefix:     data.DbData.Domain,
		Key:        data.DbData.FfUri,
		Value:      id,
		CreateTime: data.DbData.CreateTime,
		ExpireTime: data.DbData.ExpiryTime,
	}

	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)

	buf, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return -1
	}

	var ret protocal.RetTairPut
	ret.Errno, ret.Errmsg = sync.pCl.PTair.HandlerSendtoTairPut(buf)

	return ret.Errno
}

func (sync *SyncMgr) getIndexFile(prefix string, key string) *protocal.RetTairGet {
	keys := protocal.SendTairGet{
		Prefix: prefix,
		Key:    "/" + prefix + key,
	}

	var msg protocal.SednTairGetBody
	msg.Keys = append(msg.Keys, keys)

	buf, err := json.Marshal(msg)
	if err != nil {
		sync.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return nil
	}

	var ret protocal.RetTairGet
	ret.Errno, ret.Keys = sync.pCl.PTair.HandlerSendtoTairGet(buf)

	return &ret
}

func (sync *SyncMgr) getIndexFileFromFdfs(pData *protocal.DbInfo) (int, string) {
	// first get index id from tair
	ret := sync.getIndexFile(pData.Domain, pData.FfUri)
	if ret.Errno != 0 {
		sync.Logger.Errorf("ret: %+v", ret)
		return -1, ""
	}

	sync.Logger.Infof("ret: %+v", ret)

	// get index file from fdfs
	rt, fileBuff := sync.pCl.PFdfs.HandlerDownloadFile(ret.Keys[0].Value)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ""
	}

	sync.Logger.Infof("fileBuff: %+v", string(fileBuff))

	filename := pData.TaskId + pData.FileName
	file, _ := os.Create(filename)
	defer file.Close()
	file.Write(fileBuff)

	return 0, filename
}

// read binlog data from pipe
func (sync *SyncMgr) readIncreaseInfo() {
	sync.Logger.Infof("start readIncreaseInfo.")
	for {
		data := sync.pMy.Read()
		sync.Logger.Infof("data: %+v", data)
		if isOpen == 0 {
			// 给上传机发送请求，从源机器获取文件
			ret, err := sync.SendUploadServer(&data.DbData)
			if err != nil {
				sync.Logger.Errorf("SendCheckData ret:%v", ret)
			}
		} else {
			rt, filename := sync.getIndexFileFromFdfs(&data.DbData)
			if rt != 0 {
				sync.Logger.Errorf("getIndexFileFromFdfs failed, domain:%+v, prefix:+%v",
					data.DbData.Domain, data.DbData.FfUri)
				continue
			} else {
				var indexmap protocal.IndexMap
				err := indexmgr.ReadLine(filename, &indexmap)
				sync.Logger.Infof("indexmap: %+v", indexmap)
				if err != nil {
					sync.Logger.Errorf("ReadLine err:%v, filename:%+v", err, filename)
					continue
				}

				// get file buff of index and send it to backup fdfs
				sync.sendFileToBackupFdfs(&indexmap, data)
			}
		}
	}

	return
}

func (sync *SyncMgr) IncreaseSync() {
	sync.Logger.Infof("start IncreaseSync.")

	go sync.pMy.RunMoniterMysql()

	for i := 0; i < 1; i++ {
		go sync.readIncreaseInfo()
	}

	// test insert
	time.Sleep(10 * time.Second)
	//	for i := 0; i < 10; i++ {
	//		sync.pDevmgr.InsertStreamInfos(i)
	//	}

	return
}
