package org.muks.kafka.pool;

import org.muks.kafka.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class KafkaProducerPool {
    private Logger LOG = LoggerFactory.getLogger(KafkaProducerPool.class);
    private Stack<KafkaProducer> POOL;
    private int SIZE = 0;
    private static KafkaProducerPool instance = new KafkaProducerPool();


    private KafkaProducerPool() {
    }


    public static KafkaProducerPool newFixedSizePool(int size, String configDir, String configFile) {
        instance.SIZE = size;
        instance.POOL = new Stack<>();

        for (int i = 0; i < size; i++)
            instance.POOL.push(
                    new KafkaProducer()
                            .setConfigDir(configDir)
                            .setConfigFile(configFile)
                            .initialize()
            );

        return instance;
    }

    public int getSize() {
        return this.POOL.size();
    }

    public KafkaProducer get() throws Exception {
        if (this.POOL.size() != 0) {
            this.SIZE--;
            return this.POOL.pop();
        } else {
            throw new Exception("Pool is empty");
        }
    }


    public void put(KafkaProducer kafkaProducerInstance) throws Exception {
        if (this.POOL.size() == SIZE)
            this.POOL.push(kafkaProducerInstance);
        else
            throw new Exception("Pool is full");
    }


    /**
     * Shutting down by nullifying all kafka-producer instances
     */
    public void shutdown() {
        while (this.POOL.size() != 0) {
            KafkaProducer kafkaProducer = this.POOL.pop();

            if (kafkaProducer != null)
                kafkaProducer.close();

            else
                LOG.info("Is null");

        }
    }
}
