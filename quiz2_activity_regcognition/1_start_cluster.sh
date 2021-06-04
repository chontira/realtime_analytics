rm -rf /tmp/*
rm -rf logs/*

osascript -e 'tell app "Terminal"
    do script "cd /Users/new/kafka_2.13-2.7.0; ./bin/zookeeper-server-start.sh ./config/zookeeper.properties"
end tell'
sleep 3

osascript -e 'tell app "Terminal"
    do script "cd /Users/new/kafka_2.13-2.7.0; ./bin/kafka-server-start.sh ./config/server0.properties"
end tell'
osascript -e 'tell app "Terminal"
    do script "cd /Users/new/kafka_2.13-2.7.0; ./bin/kafka-server-start.sh ./config/server1.properties"
end tell'
osascript -e 'tell app "Terminal"
    do script "cd /Users/new/kafka_2.13-2.7.0; ./bin/kafka-server-start.sh ./config/server2.properties"
end tell'