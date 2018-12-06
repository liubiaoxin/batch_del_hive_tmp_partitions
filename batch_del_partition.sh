#########################################################################
## *@Company:    qd                        
## *@Prog Desc:   desc:batch drop partition
## *@Name:         drop_partitions_optimiz.sh
## *@Modify         liubiaoxin
## *@Modify Date: 2018/12/05
#########################################################################
#!/bin/bash
script_path=/home/bigdata/lbx/batch_del_hive_partitions

echo "run date: `date`;filename: $0"
echo "hive tableName:$1"
RED_COLOR='\E[1;31m'
RES='\E[0m'

#check input param
if [ $# != 3 ];then
        echo -e  "
#################################################################################################
##${RED_COLOR}======Param Error======${RES}:please check your input parameters! 
##first param:dbname 
##second param:tabname 
##three param:partitionname 
##${RED_COLOR}======example======${RES}:sh $0 test_ecall_dw_ods_fasteroms orders pt_id 
#################################################################################################"
exit 1;
fi

#hive param
DBNAME=$1
TABNAME=$2
PTNAME=$3
#MAX_PT_ID=$4

#file param
HIVEFILE="$script_path/tmp/hive_drop_partitions_${TABNAME}.hql"

if [ -a "${HIVEFILE}" ]; then
        rm -rf "${HIVEFILE}"
        echo "file exists :rm -rf '${HIVEFILE}'"
else
        touch "${HIVEFILE}"
        echo "file not exists :touch '${HIVEFILE}'"
fi

hive -e "show partitions ${DBNAME}.${TABNAME};" | grep tmp >> ${HIVEFILE}

#delete ${HIVEFILE} first line : partition
#sed -i '1d' ${HIVEFILE}

#append hql
sed -i {"s/^/alter table ${DBNAME}.${TABNAME} drop partition(/;s/$/\'\)\;/;s/=/=\'/g"} ${HIVEFILE}

hive -f "${HIVEFILE}"

#refresh table 
echo "impala-shell -q \"refresh ${DBNAME}.${TABNAME}\""
impala-shell -q "refresh ${DBNAME}.${TABNAME}"

exit;
