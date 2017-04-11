package indexmgr

import (
	"../protocal"
	"../transfer"
)

var g_indexMap = make(map[string] protocal.IndexMap)

func MapInsertItem(cl *transfer.ClusterMgr, info *protocal.IndexInfo, taskid string) {
	
	location := MapSearchItem(cl, taskid)
	if location == nil {
		var indexMap protocal.IndexMap
		indexMap.Item = append(indexMap.Item, *info)
		g_indexMap[taskid] = indexMap
	} else {
		location.Item = append(location.Item, *info)
		g_indexMap[taskid] = *location
	}
	
	// 遍历map
	for k, v := range g_indexMap {
		cl.Logger.Infof("k:%+v, v:%+v", k, v)
	}

	return
}

func MapSearchItem(cl *transfer.ClusterMgr, taskid string) *protocal.IndexMap {
	if v, ok := g_indexMap[taskid]; ok {
		cl.Logger.Infof("Find In Map, taskid:%+v, v:%+v", taskid, v)
		return &v
	} else {
		cl.Logger.Infof("taskid:%+v Key Not Found", taskid)
		return nil
	}
}

func MapDeleteItem(taskid string) {
	delete(g_indexMap, taskid)
}


