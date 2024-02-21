#!/bin/bash


for node in 1 2 4 8 16
do
    tasks=$((node * 32 * 2))
    LOG="test_log.${node}nodes.${tasks}tasks.32workers.log"
    echo "Starting test with workers=$i" &>> $LOG
    python3 test.py --workers_per_node=32 --nodes=$node --count=$tasks &>> $LOG
done
