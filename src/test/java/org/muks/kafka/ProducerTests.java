package org.muks.kafka;

import org.muks.kafka.producer.KafkaProducer;
import org.muks.kafka.pool.KafkaProducerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Unit test for simple App.
 */
public class ProducerTests {
    private static Logger LOG = LoggerFactory.getLogger(ProducerTests.class);
    private KafkaProducerPool producerPool;
    private String configDir = "/Users/mukthara/Data/git/personal/kafka-producer/configs";
    private String configFile = "producer.properties";


    @BeforeTest
    public void beforeTests() {
        producerPool = KafkaProducerPool.newFixedSizePool(1, configDir, configFile);
        producerPool.getSize();
    }


    @AfterTest
    public void afterTests() {
        LOG.info("Size: {}", producerPool.getSize());
        producerPool.shutdown();
        LOG.info("Size: {}", producerPool.getSize());
    }


    @Test
    public void PoolSizeTest() {
        int expectedSize = 1;
        int actualSize = producerPool.getSize();
        Assert.assertEquals(expectedSize, actualSize);
    }


    @Test
    public void ProduceTests() {
        LOG.info("# Will start to produce data to kafka");


        try {
            KafkaProducer kafkaProducer = producerPool.get();
            kafkaProducer.produce("I produced this...");

            producerPool.put(kafkaProducer);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
