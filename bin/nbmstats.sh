#!/bin/bash

if [ ${#1} -ne 8 ] && [ ${#2} -ne 8 ]
then
  printf  " \nThis script takes 2 command line arguments...\nSyntax: nbsstats.sh <startDate> <endDate>\nWhere startDate and endDate take the form yyyymmdd\n \n"
  exit
fi

SCRIPT_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)

python ${SCRIPT_DIR}/dlAndParseNbm.py $1 $2 &
wait
python ${SCRIPT_DIR}/dlObData.py &
wait
python ${SCRIPT_DIR}/combineAndCalculate.py &
wait