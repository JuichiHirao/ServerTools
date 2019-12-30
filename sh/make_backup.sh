#!/bin/bash
# 05 04 * * * /home/jhirao/cron_mysql_backup.sh 2> /tmp/err.log

TODAY_DATE=`date "+%Y%m%d-%H%M%S"`
MYSQL_PASSWORD=`cat cred.txt`
OUTPUT_DIR=/tmp/

# MYSQL_PWD=${MYSQL_PASSWORD} mysqldump -u admin tv > /tmp/mysql_tv_${TODAY_DATE}.dump
make_backup() {
        FILENAME=mysql_$1_${TODAY_DATE}.dump
        MYSQL_PWD=${MYSQL_PASSWORD} mysqldump -u admin $1 > ${OUTPUT_DIR}${FILENAME}
        gzip ${OUTPUT_DIR}${FILENAME}

        echo ${FILENAME}.gz
        python3 dropbox_transfer.py ${FILENAME}.gz
        rm -f ${OUTPUT_DIR}${FILENAME}
}

make_backup tv
make_backup av
make_backup scraping