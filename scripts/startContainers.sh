#!/usr/bin/env bash
if [[ $# -ne 2 ]] ; then
    echo 'You should specify 2 containers name!'
    exit 1
fi

docker start $1
docker start $2

echo "Go to container with 'docker exec -it hadoop-sql bash' command and start '/start.sh database_name2' in it"
