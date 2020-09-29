#!/bin/sh

cd /root
/usr/local/spark/sbin/start-master.sh
#./scripts/ingest_test_data.sh

./scripts/create_target_schema.sh
#sbt test
sbt package
./submit.sh
