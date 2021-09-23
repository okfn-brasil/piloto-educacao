#!/bin/bash

BACKUP_DIR="$(pwd)/downloads/backup"
if [ ! -d "$BACKUP_DIR" ]; then
  echo "You need to put snaphot on ${BACKUP_DIR}"
  exit 1
fi

docker rm -f elasticsearch || true
docker volume rm -f data-es || true
docker run -d \
           --network="host" \
           --name elasticsearch \
           -v $BACKUP_DIR:/usr/share/elasticsearch/backup \
           -v data-es:/usr/share/elasticsearch/data \
           -v $(pwd)/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml:ro \
           -e "ES_JAVA_OPTS=-Xms8g -Xmx8g" \
           docker.elastic.co/elasticsearch/elasticsearch:7.14.0

timeout 20 bash -c 'while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' localhost:9200/_snapshot)" != "200" ]]; do printf "." && sleep 1; done' || (echo "oops" && exit 1)
echo ""
echo "Elasticsearch ready"

curl -m 30 -XPUT \
  'http://localhost:9200/_snapshot/qd_repository' \
  -H 'Content-Type: application/json' -d '{
  "type": "fs",
  "settings": {
  "location": "/usr/share/elasticsearch/backup/",
  "compress": true
  }}'

echo ""
echo "Starting restore. This will take a long time"
start=`date +%s`

curl -X POST \
  'http://localhost:9200/_snapshot/qd_repository/qd_snapshot/_restore?pretty=true&wait_for_completion=true' \
  -H 'Content-Type: application/json'

end=`date +%s`
runtime=$((end-start))

echo ""
echo "($runtime seconds) run an example 'curl http://localhost:9200/queridodiario2/_search'"
