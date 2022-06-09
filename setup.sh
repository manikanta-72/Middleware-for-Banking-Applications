mkdir -p data1
mkdir -p data2
mkdir -p data3

mkdir -p data1_replicationlog
touch data1_replicationlog/replication_log.txt
mkdir -p data2_replicationlog
touch data2_replicationlog/replication_log.txt
mkdir -p data3_replicationlog
touch data3_replicationlog/replication_log.txt

docker container stop DB1
docker container stop DB2
docker container stop DB3

docker rm DB1
docker rm DB2
docker rm DB3

docker run --name DB1 --rm  \
  -e POSTGRES_PASSWORD=DB1 -p 5432:5432  \
  -v data1:/var/lib/postgresql/data \
  -v data1_replicationlog:/replication \
  -d postgres-with-python:v1

docker run --name DB2 --rm  \
  -e POSTGRES_PASSWORD=DB2 -p 5433:5432  \
  -v data2:/var/lib/postgresql/data \
  -v data2_replicationlog:/replication \
  -d postgres-with-python:v1


docker run --name DB3 --rm  \
  -e POSTGRES_PASSWORD=DB3 -p 5434:5432  \
  -v data3:/var/lib/postgresql/data \
  -v data3_replicationlog:/replication \
  -d postgres-with-python:v1