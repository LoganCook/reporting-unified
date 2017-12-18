#!/usr/bin/env bash

# Caculate offline Nova usage date of past month.
# Can be used for cron job and meant to be an example.
year=`TZ='Australia/Adelaide' date +%Y`
month=`TZ='Australia/Adelaide' date +%m`

lastmonth=$((10#$month-1))
if [ $lastmonth -eq 0 ]; then
    month=12
    year=$((year-1))
else
    month=$lastmonth
fi

source /home/ec2-user/unified_api_env/bin/activate
cd /home/ec2-user/reporting-unified/
python calculator.py -y $year -m $month nova

if [ $? ]; then
    # there should be only one file, just in case
    jsonlist=($(ls -t NovaUsage_*.json))
    if (( ${#jsonlist[@]} > 0 )); then
        sudo cp ${jsonlist[0]} /usr/share/nginx/html/reporting/usage/nova/ && sudo cp ${jsonlist[0]} /usr/share/nginx/html/institution/usage/nova/ && rm ${jsonlist[0]}
    fi
fi
