# kafka-producer
    plain kafka producer


## How to build
    mvn clean install <no tests are written as of now, so need not worry about skiping tests>

## How to run/execute
    build application jar as per "How to build" section.

    java -cp <jar-file-path> <configs-dir-path> producer.properties

    java -cp target/kafka-producer-1.0-SNAPSHOT.jar org.muks.kafka.SimpleProducer $pwd/configs producer.propertie
  
  OR 
  
    just run $] sh bin/produce.sh (Found in the repositories bin dir)
