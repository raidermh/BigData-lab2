#!/usr/bin/env bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify container name!'
    exit 1
fi

echo "Creating .jar file..."
export PATH=/opt/maven/bin:${PATH}
mvn package -f ../pom.xml

docker cp ../target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar $1:/tmp
docker cp start.sh $1:/

echo "Go to container with 'docker exec -it hadoop-sql bash' command and start '/start.sh database_name2' in it"
