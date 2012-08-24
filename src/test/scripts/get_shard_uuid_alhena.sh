#!/bin/bash

DOMS=http://alhena:7880/fedora
USER=fedoraReadOnlyAdmin
PASS=fedoraReadOnlyPass

curl --user ${USER}:${PASS} ${DOMS}'/risearch?query=select%20%24x%20%0Afrom%20%3C%23ri%3E%0Awhere%20%24x%20%3Cinfo%3Afedora%2Ffedora-system%3Adef%2Fmodel%23hasModel%3E%20%3Cinfo%3Afedora%2Fdoms%3AContentModel_Shard%3E%20minus%20%24x%20%3Cinfo%3Afedora%2Ffedora-system%3Adef%2Fmodel%23state%3E%20%3Cinfo%3Afedora%2Ffedora-system%3Adef%2Fmodel%23Deleted%3E%20minus%20%24x%20%3Chttp%3A%2F%2Fecm.sourceforge.net%2Frelations%2F0%2F2%2F%23isTemplateFor%3E%20%24y&lang=itql&format=csv&limit=0'  | grep -o "uuid.*"

