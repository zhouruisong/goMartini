package sync

import (
	"os"
	"io/ioutil"
//	"../fdfsmgr"
	"../domainmgr"
	"../protocal"
	"../indexmgr"
	"../transfer"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	log "github.com/Sirupsen/logrus"
	"time"
)

var isOpen = 0

type SyncMgr struct {
	pMy     *domainmgr.BinLogMgr
	pCl *transfer.ClusterMgr
	Logger  *log.Logger
	UploadServer  string
}

func NewSyncMgr(server string, my *domainmgr.BinLogMgr, 
	cl *transfer.ClusterMgr, lg *log.Logger) *SyncMgr {
	sy := &SyncMgr{
		pMy:     my,
		pCl: cl,
		Logger:  lg,
		UploadServer: server,
	}
	
	sy.Logger.Infof("NewSyncMgr ok")
	return sy
}

func (sync *SyncMgr) SetFlag(flag int) {
	isOpen = flag
}

func (sync *SyncMgr) SendUploadServer(data *protocal.DbInfo) (int, error) {	
	msg := protocal.UploadInfo {
		TaskId      : data.TaskId,
		Domain      : data.Domain,
		FileName    : data.FileName,
		FileType    : data.FileType,
		SourceUrl   : data.SourceUrl,
		CbUrl       : data.CbUrl,
		Behavior    : "UP",
		CreateTime  : data.CreateTime,
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

func (sync *SyncMgr) sendFileToBackupFdfs(pMap *protocal.IndexMap, data *protocal.DbInfo) int {
	for _, item := range pMap.Item {
		r, buf := sync.getFileFromFdfs(&item)
		if r == 0 {
			ret := sync.pCl.Sendbuff(buf, &item, data.TaskId)
			if ret !=0 {
				return ret
			}
		}
	}
	
//	buf, err := json.Marshal(pMap.Item)
//	if err != nil {
//		sync.Logger.Errorf("Marshal failed.err:%v, pMap: %+v", err, pMap)
//		return -1
//	}
//	
//	ret := sync.pCl.Sendbuff(buf, filename)
//	if ret != 0 {
//		sync.Logger.Errorf("sync.pCl.Sendbuff failed, ret:%+v", ret)
//		return ret
//	}
//	sync.Logger.Infof("fileBuff: %+v", string(buf))
	
	return 0
}

func (sync *SyncMgr) getFileFromFdfs(pMapItem *protocal.IndexInfo) (int, []byte) {
	var ret []byte
	// get file from fdfs
	rt, fileBuff := sync.pCl.PFdfs.HandlerDownloadFile(pMapItem.Id)
	if rt == -1 || len(fileBuff) == 0 {
		return -1, ret
	}
	
	sync.Logger.Infof("fileBuff: %+v", string(fileBuff))
			
//	filename := pData.TaskId + pData.FileName
//	file, _ := os.Create(filename)
//	defer file.Close()
//	file.Write(fileBuff)

	return 0, fileBuff
}

func (sync *SyncMgr) getIndexFileFromFdfs(pData *protocal.DbInfo) (int, string) {
	// first get index id from tair
	ret := indexmgr.GetIndexFile(sync.pCl.PTair, pData.Domain, pData.FfUri)
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

func (sync *SyncMgr) readIncreaseInfo() {
	sync.Logger.Infof("start readIncreaseInfo.")
	for {
		data := sync.pMy.Read()
		sync.Logger.Infof("data: %+v", data)
		if isOpen == 0 {
			// 给上传机发送请求，从源机器获取文件
			ret, err := sync.SendUploadServer(data)
			if err != nil {
				sync.Logger.Errorf("SendCheckData ret:%v", ret)
			}
		} else {
			rt, filename := sync.getIndexFileFromFdfs(data)
			if rt != 0 {
				continue
			} else {
				var indexmap protocal.IndexMap
				err := indexmgr.ReadLine(filename, &indexmap)
				
				sync.Logger.Infof("indexmap: %+v", indexmap)
				if err != nil {
					sync.Logger.Errorf("ReadLine err:%v", err)
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
//
	// test insert
	time.Sleep(10 * time.Second)
//	for i := 0; i < 10; i++ {
//		sync.pDevmgr.InsertStreamInfos(i)
//	}

	return
}

//func (sync *SyncMgr) TotalSync() {
//	sync.Logger.Infof("start TotalSync.")
//	tablenames, err := sync.pDevmgr.SelectTableName()
//	if err != nil {
//		return
//	}

//	for _, name := range tablenames {
//		go sync.StartTableSync(name)
//	}
//}