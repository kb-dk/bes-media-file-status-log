#!/bin/bash

EXPECTED_ARGS=1
E_BADARGS=65

if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: `basename $0` <filename>"
  echo
  echo "The refered file consists of line with uuids of the form \"uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx\""
  exit $E_BADARGS
fi

SHARD_PID_FILE=$1
PROPERTY_FILE=../conf/bes-media-file-status-log.iapetus.properties
JAR_LOCATION=../lib

# Load jobs
java -cp "${JAR_LOCATION}/*" dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.CommandLineHandler -shardpidfile $SHARD_PID_FILE -propfile $PROPERTY_FILE
