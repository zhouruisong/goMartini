#!/bin/sh
pid=`pidof binlogsync`
if [ ! -z $pid ]; then
		`kill $pid`
fi

path="/usr/local/sandai/cluster/binlogsync"
logs="$path/logs"
martini_log="$logs/access.log"
#echo "$logs"

if [ ! -d "$logs" ];then
		mkdir -p $logs
		if [ ! 0 -eq $? ];then
				echo "mkdir $logs failed."
				exit $?
		fi
fi

echo "binlogsync start."
nohup ./binlogsync --conf="../conf/conf.json" >> $martini_log &
