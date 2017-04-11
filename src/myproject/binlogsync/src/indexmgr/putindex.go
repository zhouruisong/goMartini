package indexmgr

import (
	"encoding/json"
	"../protocal"
	"../tair"
)

func PutIndexFile(pTair *tair.TairClient, prefix string, key string, id string) *protocal.RetTairGet {
	keys := protocal.SendTairPut {
		Prefix: prefix,
		Key: key,
       Value: id,
       CreateTime: 1,
       ExpireTime: 1,
	}
	
	var msg protocal.SednTairPutBody
	msg.Keys = append(msg.Keys, keys)
	
	buf, err := json.Marshal(msg)
	if err != nil {
		pTair.Logger.Errorf("Marshal failed.err:%v, msg: %+v", err, msg)
		return nil
	}
	
	var ret protocal.RetTairGet
	ret.Errno, ret.Keys = pTair.HandlerSendtoTairGet(buf)
	
	return &ret
}