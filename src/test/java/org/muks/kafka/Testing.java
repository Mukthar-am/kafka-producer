package org.muks.kafka;


import org.muks.kafka.producer.KafkaProducer;
import org.muks.kafka.pool.KafkaProducerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Testing {
    private static Logger LOG = LoggerFactory.getLogger(Testing.class);

    public static void main(String[] args) {
        String configDir = "/Users/mukthara/Data/git/personal/kafka-producer/configs";
        String configFile = "producer.properties";

        KafkaProducerPool producerPool = KafkaProducerPool.newFixedSizePool(5, configDir, configFile);
        producerPool.getSize();

        try {
            KafkaProducer kafkaProducer = producerPool.get();
            kafkaProducer.produce("I produced this...");

            producerPool.put(kafkaProducer);

        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("Size: {}", producerPool.getSize());
        producerPool.shutdown();

        LOG.info("Size: {}", producerPool.getSize());
    }

}
