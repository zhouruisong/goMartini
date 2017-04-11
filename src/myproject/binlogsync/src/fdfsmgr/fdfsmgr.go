package fdfsmgr

import (
	"../fdfs_client"
	log "github.com/Sirupsen/logrus"
)

/////////////////////////////////////////////////////////
// 向fastdfs存储数据请求接口
type CentreUploadFile struct {
	Taskid   string     `json:"taskid"`
	Index    IndexInfo   `json:"index"`
	Content  []byte     `json:"content"`
}

// 向fastdfs存储数据返回接口
type RetCentreUploadFile struct {
	Errno  int        `json:"code"`
	Errmsg string     `json:"message"`
	Id     string     `json:"id"`
}

// 向fastdfs下载数据请求接口
type CentreDownloadFile struct {
	Id     string     `json:"id"`
}

// 向fastdfs下载数据请求接口
type CentreDownloadFileEx struct {
	Logid    string     `json:"logid"`
	Id     string     `json:"id"`
}

// 向fastdfs下载数据返回接口
type RetCentreDownloadFile struct {
	Errno  int        `json:"code"`
	Errmsg string     `json:"message"`
	Content  []byte   `json:"content"`
}
/////////////////////////////////////////////////////////

type FdfsMgr struct {
	pFdfs   *fdfs_client.FdfsClient
	Logger  *log.Logger
}

func NewClient(trackerlist []string, lg *log.Logger, minConns int, maxConns int) *FdfsMgr {
	pfdfs, err := fdfs_client.NewFdfsClient(trackerlist, lg, minConns, maxConns)
	if err != nil {
		lg.Errorf("NewClient failed")
		return nil
	}
	
	fd := &FdfsMgr{
		pFdfs:   pfdfs,
		Logger:  lg,
	}
	fd.Logger.Infof("NewClient ok")
	return fd
}

func (fdfs *FdfsMgr) HandlerDownloadFile(id string) (int, []byte) {
	var ret_buf []byte	
	downloadResponse, err := fdfs.pFdfs.DownloadToBuffer(id, 0, 0)
	if err != nil {
		fdfs.Logger.Errorf("DownloadToBuffer fail, err:%v, id:%+v", 
			err, id)
		return -1, ret_buf
	}

	if value, ok := downloadResponse.Content.([]byte); ok {
		fdfs.Logger.Infof("DownloadToBuffer ok id:%+v", id)
		return 0, value
	}

	return -1, ret_buf
}

// 处理函数
func (fdfs *FdfsMgr) HandlerUploadFile(buf []byte) (int, string) {
	if len(buf) == 0 {
		fdfs.Logger.Errorf("handlerUploadFile buf len = 0")
		return -1, ""
	}

	uploadres, err := fdfs.pFdfs.UploadAppenderByBuffer(buf, "")
	if err != nil {
		fdfs.Logger.Errorf("UploadAppenderByBuffer failed err:%v", err)
		return -1, ""
	}

	fdfs.Logger.Infof("UploadAppenderByBuffer ok uploadres:%+v", uploadres)
	return 0, uploadres.RemoteFileId
}
