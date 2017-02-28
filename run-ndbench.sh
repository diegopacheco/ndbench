#/bin/bash

export DYNOMITE_SEEDS="127.0.0.1:8102:rack1:dc:100";

./gradlew farmStartWar > ndbench-log.log  2>&1 &
sleep 3

PID="PID :  $(pgrep java)"
echo $PID
echo $PID >> ndbench-log.log

tail -f ndbench-log.log
