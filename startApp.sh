#!/bin/bash
cd app

uvicorn main:app --reload --port 4304 --host 0.0.0.0 &
pid1=$!
echo "started proc1: ${pid1}"


python3 scheduler.py &
pid2=$!
echo "started proc2: ${pid2}"

python3 runner.py &
pid3=$!
echo "started proc3: ${pid3}"

trap "kill -9 $pid1 $pid2 ${pid3}" SIGINT
wait
