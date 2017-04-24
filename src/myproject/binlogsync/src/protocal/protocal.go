package protocal

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

// send mysql data to other
type DbEventInfo struct {
	TableName string
	DbData     DbInfo
}

// index
type IndexInfo struct {
	Name   string `json:"name"`
	Id     string `json:"id"`
	Status string `json:"status"`
	Size   string `json:"size"`
	Md5    string `json:"md5"`
}

// sync
type IndexMap struct {
	Item []IndexInfo `json:"indexmap"`
}

type RetUploadMeg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type UploadInfo struct {
	TaskId     string `json:"taskid"`
	Domain     string `json:"domain"`
	FileName   string `json:"fname"`
	FileType   string `json:"ftype"`
	SourceUrl  string `json:"url"`
	CbUrl      string `json:"cb_url"`
	Behavior   string `json:"behavior"`
	CreateTime string `json:"createtime"`
}

type InsertInfo struct {
	Id         int32
	TaskId     string
	Domain     string
	FileName   string
	FileType   string
	SourceUrl  string
	CbUrl      string
	CreateTime string
}

// sync

// mysql db live_master table
type DbRowInfo struct {
	TaskId     string
	FileName   string
	FileType   string
	Domain     string
	Status     int32
	SourceUrl  string
	CbUrl      string
	CreateTime string
	FfUri      string
}

/////////////////////////////////////////////////////////
// 向fastdfs存储数据请求接口
type CentreUploadFile struct {
	Taskid  string    `json:"taskid"`
	Content []byte    `json:"content"`
}

// 向fastdfs存储数据返回接口
type RetCentreUploadFile struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
	Id     string `json:"id"`
}

// 向fastdfs下载数据请求接口
type CentreDownloadFile struct {
	Id string `json:"id"`
}

// 向fastdfs下载数据返回接口
type RetCentreDownloadFile struct {
	Errno   int    `json:"code"`
	Errmsg  string `json:"message"`
	Content []byte `json:"content"`
}

// 向fastdfs下载数据请求接口
type CentreDeleteFile struct {
	Id string `json:"id"`
}

// 向fastdfs存储数据返回接口
type RetCentreDeleteFile struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

/////////////////////////////////////////////////////////

// return of sending mysql data to other
type MsgInsertRet struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

// send mysql data to other
type MsgInsertBody struct {
	TableName string `json:"tablename"`
	Data      DbInfo `json:"data"`
}

//get inferface
////////////////////////////////////////////////////////
type SendTairGet struct {
	Prefix string `json:"prefix"`
	Key    string `json:"key"`
}
type SednTairGetBody struct {
	Keys []SendTairGet `json:"keys"`
}
type SendTairMesageGet struct {
	Command    string        `json:"command"`
	ServerAddr string        `json:"server_addr"`
	GroupName  string        `json:"group_name"`
	Keys       []SendTairGet `json:"keys"`
}
type RetTairGetDetail struct {
	Prefix     string `json:"prefix"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	CreateTime string `json:"createtime"`
	ExpireTime string `json:"expiretime"`
}
type RetTairGet struct {
	Errno  int                `json:"code"`
	Errmsg string             `json:"message"`
	Keys   []RetTairGetDetail `json:"keys"`
}
type RetTairGetKeys struct {
	Keys []RetTairGetDetail `json:"keys"`
}

//put interface
////////////////////////////////////////////////////////
type SendTairPut struct {
	Prefix     string `json:"prefix"`
	Key        string `json:"key"`
	Value      string `json:"value"`
	CreateTime string `json:"createtime"`
	ExpireTime string `json:"expiretime"`
}
type SednTairPutBody struct {
	Keys []SendTairPut `json:"keys"`
}
type SendTairMesage struct {
	Command    string        `json:"command"`
	ServerAddr string        `json:"server_addr"`
	GroupName  string        `json:"group_name"`
	Keys       []SendTairPut `json:"keys"`
}
type RetTairPut struct {
	Errno  int    `json:"code"`
	Errmsg string `json:"message"`
}

/////////////////////////////////////////////////////////
