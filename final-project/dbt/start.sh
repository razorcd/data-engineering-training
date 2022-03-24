#!/bin/bash

let i=0
while true
do
   let i++
   echo "Starting DBT run $i"
   dbt run --profiles-dir /dbt --full-refresh
   sleep 60m  
done