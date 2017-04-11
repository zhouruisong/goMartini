package indexmgr

import (
	"encoding/json"
	"../protocal"
	"../tair"
)

func GetIndexFile(pTair *tair.TairClient, prefix string, key string) *protocal.RetTairGet {
	keys := protocal.SendTairGet {
		Prefix: prefix,
		Key: "/" + prefix + key,
	}
	
	pTair.Logger.Infof("keys: %+v", keys)
	
	var msg protocal.SednTairGetBody
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