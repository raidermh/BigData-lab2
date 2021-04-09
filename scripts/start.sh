#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify database name!'
    exit 1
fi


export PATH=$PATH:/usr/local/hadoop/bin/
hadoop dfs -rm -r logs
hadoop dfs -rm -r out

# Устанавливаем PostgreSQL
sudo apt-get update -y
sudo apt-get install -y postgresql postgresql-contrib
sudo service postgresql start

# Создаем таблицу с логами
sudo -u postgres psql -c 'ALTER USER postgres PASSWORD '\''1234'\'';'
sudo -u postgres psql -c 'drop database if exists '"$1"';'
sudo -u postgres psql -c 'create database '"$1"';'
sudo -u postgres -H -- psql -d $1 -c 'CREATE TABLE logging (id BIGSERIAL PRIMARY KEY, numberFlight int, datetime VARCHAR(20), source VARCHAR(10), destination VARCHAR(10));'

# Генерируем входные данные и добавляем их в таблицу
SOURCE=("DME"
			  "TXL"
			  "MXP"
			  "MAD"
			  "PRG")

DESTINATION=("IST"
			       "KBP"
			       "LIS"
			       "ATH"
			       "BUD")

for i in {1..200}
	do
	    HOUR=$((RANDOM % 24))
	    if [ $HOUR -le 9 ]; then
	        TWO_DIGIT_HOUR="0$HOUR"
	    else
	        TWO_DIGIT_HOUR="$HOUR"
	    fi
		sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO logging (numberFlight, datetime, source, destination) values ('"$((RANDOM % 8))"','\''Nov 10 '"$TWO_DIGIT_HOUR"':13:56'\'','\'"${SOURCE[$((RANDOM % ${#SOURCE[*]}))]}"\'','\'"${DESTINATION[$((RANDOM % ${#DESTINATION[*]}))]}"\'');'
	done

# Скачиваем SQOOP
if [ ! -f sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz ]; then
    wget http://apache-mirror.rbc.ru/pub/apache/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
    tar xvzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
else
    echo "Sqoop already exists, skipping..."
fi

# Скачиваем драйвер PostgreSQL
if [ ! -f postgresql-42.2.5.jar ]; then
    wget https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
    cp postgresql-42.2.5.jar sqoop-1.4.7.bin__hadoop-2.6.0/lib/
else
    echo "Postgresql driver already exists, skipping..."
fi

export PATH=$PATH:/sqoop-1.4.7.bin__hadoop-2.6.0/bin

# Скачиваем Spark
if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi

export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop

sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"$1"'?ssl=false' --username 'postgres' --password '1234' --table 'logging' --target-dir 'logs'

export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin

spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 1g --name wordcount --conf "spark.app.id=SparkSQLApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar cfs://127.0.0.1:9000/user/root/logs/ out

echo "DONE! RESULT IS: "
hadoop fs -cat  cfs://127.0.0.1:9000/user/root/out/part-00000