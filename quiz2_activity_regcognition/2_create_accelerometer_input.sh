cd /Users/new/kafka_2.13-2.7.0
./bin/kafka-topics.sh --bootstrap-server "localhost:9092,localhost:9192,localhost:9292" --create --replication-factor 3 --partitions 1 --topic accelerometer-input