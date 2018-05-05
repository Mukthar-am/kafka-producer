
pwd=`pwd`
echo $pwd
cmd="java -cp $pwd/target/kafka-producer-1.0-SNAPSHOT.jar org.muks.kafka.producer.SimpleProducer $pwd/configs producer.properties"

$cmd
