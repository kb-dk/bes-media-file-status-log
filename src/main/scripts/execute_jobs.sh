#!/bin/bash

PROPERTY_FILE=../conf/bes-media-file-status-log.iapetus.properties
JAR_LOCATION=../lib

# Execute loaded jobs
java -cp "${JAR_LOCATION}/*" dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.CommandLineHandler -execute_jobs -propfile $PROPERTY_FILE
