#for local runs

docker exec -it spark bash
cd /app
spark-submit spark_jdbc_sink_clickhouse.py

