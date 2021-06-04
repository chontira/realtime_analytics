#!/bin/bash
function pause(){
   read -p "$*"
}

cd /Users/new/kafka-2.7.0-src
gradle assemble -x clients:javadoc streams:test-utils:javadoc streams:streams-scala:scaladoc connect:mirror-client:javadoc connect:api:javadoc core:javadoc core:compileScala
pause 'Press [Enter] key to continue...'

cp "/Users/new/kafka-2.7.0-src/streams/examples/build/libs/kafka-streams-examples-2.7.0.jar" "/Users/new/kafka_2.13-2.7.0/libs"

osascript -e 'tell app "Terminal"
    do script "cd /Users/new/kafka_2.13-2.7.0; ./bin/kafka-run-class.sh org.apache.kafka.streams.examples.tfidf.tfidfDemo"
end tell'