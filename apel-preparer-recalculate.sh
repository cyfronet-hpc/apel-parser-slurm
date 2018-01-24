#!/bin/bash

DAY=$(date +"%Y-%m-%d")

while :; do
	if [ "$DAY" == "2017-09-15" ]; then
		break
	fi
	DAY=$(date --date=@$(( $(date --date=${DAY} +"%s") - 24*60*60 )) +"%Y-%m-%d")
	echo "$DAY"
	/opt/cyfronet/bin/apel-preparer.py -D -c -s -d "${DAY}"
	sleep 1
done
