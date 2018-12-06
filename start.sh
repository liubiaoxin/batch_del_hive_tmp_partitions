#!/bin/bash
script_path=/home/bigdata/lbx/batch_del_hive_partitions

for line in `cat $script_path/hive_tables.conf`
do
   array=(${line//,/ })
   schema="${array[0]}"
   table_name=${array[1]}
   echo "sh $script_path/batch_del_partition.sh  $schema  $table_name 'test'"
   sh $script_path/batch_del_partition.sh  $schema  $table_name 'test' 
done

