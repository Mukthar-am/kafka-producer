# kafka-producer
    plain kafka producer


## How to build
    mvn clean install <no tests are written as of now, so need not worry about skiping tests>

## How to run/execute
    build application jar as per "How to build" section.

    java -cp <jar-file-path> <configs-dir-path> producer.properties

    java -cp target/kafka-producer-1.0-SNAPSHOT.jar org.muks.kafka.producer.KafkaProducer -configDir /Users/mukthara/Data/git/personal/kafka-producer/configs -configFile producer.properties -delay 5000
  
  OR 
  
    just run $] sh bin/produce.sh (Found in the repositories bin dir)
