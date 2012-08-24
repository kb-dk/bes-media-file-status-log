#!/bin/bash

SHARD_PID_FILE=src/test/resources/testfiles/shard_uuids_alhena.txt

# Local SQLite db
#PROPERTY_FILE=src/test/config/bes_media_file_log_batch_update_test_app.properties

# Iapetus postgres
PROPERTY_FILE=src/test/config/bes_media_file_log_batch_update_test_iapetus_db.properties

JAR_LOCATION=target

# Simpler !!!!
# java -jar target/bes-media-file-status-log-1.0-SNAPSHOT-jar-with-dependencies.jar 

# Load jobs
#java -cp ${JAR_LOCATION}/bes-media-file-status-log-*-jar-with-dependencies.jar:${JAR_LOCATION}/* dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.CommandLineHandler -shardpidfile $SHARD_PID_FILE -propfile $PROPERTY_FILE
# java -jar target/bes-media-file-status-log-1.0-SNAPSHOT-jar-with-dependencies.jar -shardpidfile $SHARD_PID_FILE -propfile $PROPERTY_FILE

# Execute loaded jobs
#java -cp ${JAR_LOCATION}/bes-media-file-status-log-*-jar-with-dependencies.jar:${JAR_LOCATION}/* dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.CommandLineHandler -execute_jobs -propfile $PROPERTY_FILE
#java -jar target/bes-media-file-status-log-1.0-SNAPSHOT-jar-with-dependencies.jar -execute_jobs -propfile $PROPERTY_FILE

# Load jobs and execute loaded jobs
#java -cp ${JAR_LOCATION}/bes-media-file-status-log-*-jar-with-dependencies.jar:${JAR_LOCATION}/* dk.statsbiblioteket.mediaplatform.bes.mediafilelog.batch.CommandLineHandler -shardpidfile $SHARD_PID_FILE -execute_jobs -propfile $PROPERTY_FILE
java -jar target/bes-media-file-status-log-1.0-SNAPSHOT-jar-with-dependencies.jar -shardpidfile $SHARD_PID_FILE -execute_jobs -propfile $PROPERTY_FILE