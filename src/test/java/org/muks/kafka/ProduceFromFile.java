package org.muks.kafka;


import org.muks.kafka.pool.KafkaProducerPool;
import org.muks.kafka.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;

public class ProduceFromFile {
    private static Logger LOG = LoggerFactory.getLogger(ProduceFromFile.class);
    private static boolean KEEP_RUNNING = true;
    private static int DELAY = 5000;
    private static int LINES_PRODUCED = 0;

    public static void main(String[] args) {
        String configDir = "/Users/mukthara/Data/git/personal/kafka-producer/configs";
        String configFile = "producer.properties";
        String contentFile = "clickstream-sample.json";

        KafkaProducerPool producerPool = KafkaProducerPool.newFixedSizePool(5, configDir, configFile);
        producerPool.getSize();


        try {
            KafkaProducer kafkaProducer = producerPool.get();

            while (KEEP_RUNNING) {
                publishFileContent(configDir + "/" + contentFile, kafkaProducer);

                if (DELAY > 0)
                    Thread.sleep(DELAY);
            }

            producerPool.put(kafkaProducer);

        } catch (Exception e) {
            e.printStackTrace();
        }


        LOG.info("Size: {}", producerPool.getSize());
        producerPool.shutdown();

        LOG.info("Size: {}", producerPool.getSize());
    }


    private static void publishFileContent(String contentFile, KafkaProducer kafkaProducer) {

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(contentFile));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                kafkaProducer.produce(line);
                LINES_PRODUCED++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("Lines produced so far = {}", LINES_PRODUCED);
    }

}
