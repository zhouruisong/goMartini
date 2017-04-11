#!/bin/sh
pid=`pidof binlogsync`
if [ ! -z $pid ]; then
		echo "kill pid $pid"
		`kill $pid`
fi
echo "binlogsync stop."

