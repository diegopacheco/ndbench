#/bin/bash

export DYNOMITE_SEEDS="127.0.0.1:8101:rack1:100:dc";

./gradlew farmStartWar > ndbench-log.log &
PID="PID :  $(pgrep gradlew) "
echo $PID
echo $PID >> ndbench-log.log

tail -f ndbench-log.log
