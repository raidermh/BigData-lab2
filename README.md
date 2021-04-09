Инструкция по сборке и развертыванию
1.	Подключить репозиторий yum install -y epel-release.
2.	Установить docker командой curl -fsSL http://get.docker.com|sh.
3.	Запустить демона docker командой systemctl start docker.
4.	Убедиться, что демон docker запустился командой systemctl status docker.service.
5.	Делаем так, чтобы докер запускался по-умолчанию всегда  командной systemctl enable docker.
6.	Загрузить контейнер с Cassandra командой docker pull cassandra:4.0 и контейнер с hadoop командой docker pull zoltannz/hadoop-ubuntu:2.8.1.
7.	Собирать и запустить наши контейнеры командами: 
docker run --name hadoop-sql -p 2122:2122 -p 8020:8020 -p 8030:8030 -p 8040:8040 -p 8042:8042 -p 8088:8088 -p 9000:9000 -p 10020:10020 -p 19888:19888 -p 49707:49707 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -p 50090:50090 -t hadoop-ubuntu:2.8.1
docker run --name cassandra-sql -p 7199:7199 -p 7000:7000 -p 7001:7001 -p 9160:9160 -p 9042:9042 -t cassandra:4.0
8.	Остановить контейнеры для подготовки программной реализации Spark, командами docker stop cassandra-sql и docker stop hadoop-sql.
9.	Запустить контейнеры скриптом ./startContainers.sh hadoop-sql cassandra sql.
10.	Собирать проект в intellij idea, с помощью maven.
11.	Скопировать lab2-1.0-SNAPSHOT-jar-with-dependencies.jar в контейнер hadoop-sql, командой docker cp ../target/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-sql:/tmp
12.	Скопировать скрипт запуска в контейнер, командой docker cp start.sh hadoop-sql:/
13.	Перейти в контейнер docker exec -it hadoop-sql bash
14.	Запустить скрипт /start.sh database_name2
[bigDataComp.pdf](https://github.com/raidermh/BigData-lab2/files/6288031/bigDataComp.pdf)
