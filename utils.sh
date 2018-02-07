#!/bin/bash

# Utility functions

function formatDateFromEpoch ()
{
    # Check that we have all parameter
    if [[ $# -ne 1 ]]; then
        echo 1>&2 "ERROR: $0: Number of parameters incorrect, expected 1 and got: $#"
        return 1
    fi

    dateEpoch=$1

    if [[ "$(uname)" == "Darwin" ]]; then
        # It is a mac
        # Extract data
        dateFormatted=`date -jf "%s" $dateEpoch "+%Y%m%d %H:%M:%S"`
    else
        # Extract data
        dateFormatted=`date -d@$dateEpoch +"%Y%m%d %H:%M:%S"`
    fi
}

function checkIsHdfsDirectoryAndCreate ()
{
    # Check that we have all parameters
    if [[ $# -ne 1 ]]; then
        echo 1>&2 "ERROR: $0: Number of parameters incorrect, expected 1 and got: $#"
        return 1
    fi

    hdfs dfs -test -d $1

    if [[ $? == 0 ]]; then
        echo 1>&2 "INFO: $0: Folder $1 exists."
        return 0
    else
        echo 1>&2 "INFO: $0: Folder $1 doesn't exist, creating..."
        hdfs dfs -mkdir -p $1
        if [[ "$?" -ne "0" ]]; then
              echo 1>&2 "ERROR: $0: $1 could not be created, please check it"
            return 2
        else
            echo 1>&2 "INFO: $0: Folder $1 created successfully."
            return 1
        fi

    fi
}

function checkIsNonEmptyHdfsDir ()
{
    # Check that we have exactly one parameter
    if [[ $# -ne 1 ]]; then
        echo 1>&2 "ERROR: $0: Directory name has to be specified"
        # Exit entire script
        exit 1
    fi

    isEmpty=$(hdfs dfs -count "$1" | awk '{print $2}')

    if [[ $isEmpty -eq 0 ]];then
        echo  1>&2 "WARNING: $0: Target HDFS directory $1 is empty"
    else
        echo  1>&2 "INFO: $0: Target HDFS directory $1 is non-empty"
    fi
}
