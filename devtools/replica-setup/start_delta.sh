#!/bin/bash

# Use the EXECUTED_GTID_SET variable from the previous steps
if [ -z "$EXECUTED_GTID_SET" ]; then
  echo "Executed_GTID_set is empty, exiting."
  exit 1
fi

# Connect to MySQL and execute the replication configuration commands
mysql -h127.0.0.1 -uroot -P3306 <<EOF
SET global gtid_purged = "${EXECUTED_GTID_SET}";
CHANGE REPLICATION SOURCE TO SOURCE_HOST='${MYSQL_HOST}',
  SOURCE_PORT=${MYSQL_PORT},
  SOURCE_USER='${MYSQL_USER}',
  SOURCE_PASSWORD='${MYSQL_PASSWORD}'
;
START REPLICA;
EOF

# Check if the commands were successful
if [ $? -ne 0 ]; then
  echo "Failed to start replication. Exiting."
  exit 1
else
  echo "Replication started successfully."
fi