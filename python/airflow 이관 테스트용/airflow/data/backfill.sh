#!/bin/bash
START=`date -j -f %Y-%m-%d:%H.%M $1:0.0 +%s`
END=`date -j -f %Y-%m-%d:%H.%M $2:0.0 +%s`

DAG=$3
TASK=$4

echo $DAG
for (( i=$START; i<=$END; i+=86400 )); do # seconds/day
   DATE=`date -j -f %s "$i" +%Y%m%d`
   echo `gcloud composer environments run tada-airflow --location us-east1 test -- -sd /home/airflow/gcs/dags/ ${DAG} ${TASK} ${DATE}`
done;  
