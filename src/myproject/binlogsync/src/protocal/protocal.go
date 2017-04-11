package protocal

// index
type IndexInfo struct {
	Name    string   `json:"name"`
	Id      string   `json:"id"`
	Status  string   `json:"status"`
	Size    string   `json:"size"`
	Md5     string   `json:"md5"`
}

// sync
type IndexMap struct {
	Item  []IndexInfo  `json:"indexmap"`
}

type RetUploadMeg struct {
	Code       int    	`json:"code"`
	Message    string    	`json:"message"`
}

type UploadInfo struct {
	TaskId       string    	`json:"taskid"`
	Domain       string    	`json:"domain"`
	FileName     string    	`json:"fname"`
	FileType     string    	`json:"ftype"`
	SourceUrl    string    	`json:"url"`
	CbUrl        string    	`json:"cb_url"`
	Behavior     string    	`json:"behavior"`
	CreateTime   string      `json:"createtime"`
}

type InsertInfo struct {
	Id           int32
	TaskId       string 
	Domain       string 
	FileName     string 
	FileType     string 
	SourceUrl    string 
	CbUrl        string
	CreateTime   string 
}
// sync

// mysql db live_master table 
type DbRowInfo struct {
	TaskId       string 
	FileName     string 
	FileType     string 
	Domain       string
	Status       int32
	SourceUrl    string 
	CbUrl        string
	CreateTime   string
	FfUri        string
}

//time
type TimeInfo struct
{
	hour int
	minute int
	second int
}