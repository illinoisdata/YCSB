LOG_FOLDER=./cruzdb-logs
DATA_FOLDER=db2
RECORD_COUNT=100000

export LD_LIBRARY_PATH=/usr/local/lib
export ZLOG_LMDB_BE_SIZE=10


#for WORKLOAD in workloada workloadb workloadc workloadd
for WORKLOAD in workloadc
do
    if [ $1 = "l" ];then
       rm -f ${DATA_FOLDER}/*
       mkdir -p ${DATA_FOLDER}
       mkdir -p ${LOG_FOLDER}
       ./bin/ycsb load cruzdb -threads 1 -s -P workloads/${WORKLOAD} -p recordcount=${RECORD_COUNT} -p cruzdb.lmdb.dir=$PWD/db2 -p load.first=true | tee ${LOG_FOLDER}/load_${WORKLOAD}_logs.txt
    else
       ./bin/ycsb run cruzdb -threads 1 -s -P workloads/${WORKLOAD} -p recordcount=${RECORD_COUNT} -p cruzdb.lmdb.dir=$PWD/db2 -p load.first=true | tee ${LOG_FOLDER}/run_${WORKLOAD}_logs.txt
    fi

done
