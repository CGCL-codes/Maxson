#!/bin/sh
if [ $# -lt 2 ];then
        echo "sh run.sh <full_qualified_class_name> <optimizeUsed> <tableName> <cycleTimes> [1:tail -f log]"
        exit 1
else
        full_qualified_class_name=$1
        jar_path=/c/Users/zyp/ali/spark/examples/target/original-spark-examples_2.11-2.3.0.jar
        class_name=`echo ${full_qualified_class_name##*.}`
        time_now=`date +%y_%d_%m:%T`
        equal="==========================="
        log_dir="logs/$class_name-$time_now.log"
        echo "full_qualified_class_name: " "${full_qualified_class_name}" | tee -a $log_dir
        echo "jar_path: " "${jar_path}" | tee -a $jar_path
        echo "class_name: " "${class_name}"| tee -a $log_dir
        echo "time_now:" "${time_now}"| tee -a $log_dir
        
        optimizeUsed=$2
        tableName=$3
        cycleTimes=$4
        echo "optimizeUsed: ${optimizeUsed}" "tableName:${tableName}" "cycleTimes:${cycleTimes}" | tee -a $log_dir
        exec_command="nohup ./bin/spark-submit --master local  --class $full_qualified_class_name $jar_path -o '$optimizeUsed' -t '$tableName' -n '$cycleTimes' >> $log_dir 2>&1 &"
        
        echo "exec_command: $exec_command" | tee -a $log_dir
        eval ${exec_command}
        if [ "$4" = "1" ];then
            echo "$equal" "log content" "$equal"
            tail -f $log_dir 
        fi
            
fi
