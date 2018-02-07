#!/bin/bash

# Start time stamp
startTimestampUtc=`date -u +%s`

# Get process PID
PROCESS_PID=$$

# Get base dir name
BASE_DIR=`dirname "${0}"`

function formatDateFromEpoch ()
{
    # Check that we have all parameter
    if [ $# -ne 1 ]; then
        echo 1>&2 "ERROR: $0: Number of parameters incorrect, expected 1 and got: $#"
        return 1
    fi

    dateEpoch=$1

    if [ "$(uname)" == "Darwin" ]; then
        # It is a mac
        # Extract data
        dateFormatted=`date -jf "%s" $dateEpoch "+%Y%m%d %H:%M:%S"`
    else
        # Extract data
        dateFormatted=`date -d@$dateEpoch +"%Y%m%d %H:%M:%S"`
    fi
}

. utils.sh

function executeHiveQueryAndCaptureResult ()
{
    sql_string = $1
    res_val=$(hive -S -e $sql_string | cut -f 1)
    return res_val
}

# main programme

### global variables
TIMESTAMP=`date +%Y-%m-%d_%H:%M:%S`
USER="$(whoami)"
OUTPUT_FILE="/home/${USER}/hive_test_$TIMESTAMP.out"
LOG_FILE="/home/${USER}/hive_test_$TIMESTAMP.out"

### user input
# echo "Please enter the build version: "
# read BUILD_VERSION
# echo "Please enter the database name: "
# read DATABASE
# echo "Please enter the date for testing: "
# read DADATE

### directory

PREFIX="/some/path/to/tables"
ONE_DATA_PATH="$PREFIX/one"
TWO_DATA_PATH="$PREFIX/two"
THREE_DATA_PATH="$PREFIX/three"
FOUR_DATA_PATH="$PREFIX/four"
FIVE_DATA_PATH="$PREFIX/five"

### echo script parameters

echo 1<&2 "INFO: ${0}: Running with following parameters: "
echo 1>&2 "    script base directory: ${BASE_DIR}"
echo 1>&2 "    build version: ${BUILD_VERSION}"
echo 1>&2 "    release version: ${RELEASE_VERSION}"
echo 1>&2 "    user: ${USER}"
echo 1>&2 "    analysis date: ${DADATE}"
echo 1>&2 "    target database: ${DATABASE}"
echo 1>&2 "    hdfs root dir: ${PREFIX}"
echo 1>&2 "    enriched data path: ${ONE_DATA_PATH}"
echo 1>&2 "    trips data path: ${TWO_DATA_PATH}"
echo 1>&2 "    trips location data path: ${THREE_DATA_PATH}"
echo 1>&2 "    trips activity data path: ${FOUR_DATA_PATH}"
echo 1>&2 "    root summary data path: ${FIVE_DATA_PATH}"

### check hdfs dirs

checkIsHdfsDirectoryAndCreate "${PREFIX}"
checkIsHdfsDirectoryAndCreate "${ONE_DATA_PATH}"
checkIsHdfsDirectoryAndCreate "${TWO_DATA_PATH}"
checkIsHdfsDirectoryAndCreate "${THREE_DATA_PATH}"
checkIsHdfsDirectoryAndCreate "${FOUR_DATA_PATH}"

### check hdfs dir size

checkIsNonEmptyHdfsDir "${PREFIX}"
checkIsNonEmptyHdfsDir "${ONE_DATA_PATH}"
checkIsNonEmptyHdfsDir "${TWO_DATA_PATH}"
checkIsNonEmptyHdfsDir "${THREE_DATA_PATH}"
checkIsNonEmptyHdfsDir "${FOUR_DATA_PATH}"



### Hive Config
echo "use ${DATABASE};" >> tmp_runner.hql
### Start of Hive Queries
# Some Table
echo "select columnName from tableName where condition;" >> tmp_runner.hql
echo "select count(*) from tableName where condition;" >> tmp_runner.hql

# Write results to file
hive -v -f tmp_runner.hql &> $OUTPUT_FILE

### End of Hive Queries
echo "$DATETIME: End of queries"
# Remove tmp_runner.hql
echo "$DATETIME: Removing tmp_runner.hql..."
rm -f tmp_runner.hql

# End time stamp
endTimestampUtc=`date -u +%s`

# Calculate duration
duration=`expr ${endTimestampUtc} - ${startTimestampUtc}`
formatDateFromEpoch ${startTimestampUtc}
startFormatted=$dateFormatted

formatDateFromEpoch ${endTimestampUtc}
endFormatted=$dateFormatted

# Write summary of execution
echo 1<&2 "INFO: ${0}: Finished process in: $duration seconds, started at: ${startFormatted}, finished at: ${endFormatted}"
