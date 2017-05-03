#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/config.sh"
. "${DIR}/bin/config.sh"

echo "========== running ${APP} bench =========="


# pre-running
DU ${INPUT_HDFS} SIZE

JAR="${DIR}/target/KMeansApp-1.0.jar"
CLASS="KmeansApp"
OPTION=" ${INOUT_SCHEME}${INPUT_HDFS} ${INOUT_SCHEME}${OUTPUT_HDFS} ${NUM_OF_CLUSTERS} ${MAX_ITERATION} ${STORAGE_LEVEL} ${NUM_RUN}"


setup
set_gendata_opt
for((i=0;i<${NUM_TRIALS};i++)); do
    RM ${OUTPUT_HDFS}
    purge_data "${MC_LIST}"
    START_TS=`get_start_ts`;
    START_TIME=`timestamp`

    mkdir -p "out/$i"
    json_log="out/$i/${STORAGE_LEVEL}-${NUM_OF_POINTS}.json"
    run_with_stats $json_log sh -c " ${SPARK_HOME}/bin/spark-submit --class $CLASS --master ${APP_MASTER} ${YARN_OPT} ${SPARK_OPT} ${SPARK_RUN_OPT} $JAR $json_log ${OPTION} 2>&1|tee ${BENCH_NUM}/${APP}_run_${START_TS}.dat" $json_log
    res=$?;


    END_TIME=`timestamp`
    get_config_fields >> ${BENCH_REPORT}
    print_config  ${APP} ${START_TIME} ${END_TIME} ${SIZE} ${START_TS} ${res}>> ${BENCH_REPORT};
done
teardown

exit 0

