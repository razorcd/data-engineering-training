#!/bin/bash

let i=0
while true
do
   let i++
   echo "Starting Spark run $i."
   python main.py
   echo "Run $i complete. Waiting 20min for next run."
   sleep 20m  
done