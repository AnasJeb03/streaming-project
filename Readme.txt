py -3.13 client/client.py 
docker-compose up -d
docker exec -d spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 --master local[*] /opt/spark_jobs/spark_streaming_mongodb.py
streamlit run streamlit/dashboard.py
docker exec -it kafka kafka-topics --create --topic invoices-input --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1