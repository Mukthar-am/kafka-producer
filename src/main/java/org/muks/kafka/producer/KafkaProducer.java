package org.muks.kafka.producer;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaProducer {
    private Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
    private org.apache.kafka.clients.producer.Producer PRODUCER = null;
    private String TOPIC;
    private int ITEMS_PRODUCED = 0;
    private String CONFIG_DIR;
    private String PROPS_FILE;


    public KafkaProducer setConfigDir(String configDir) {
        this.CONFIG_DIR = configDir;
        return this;
    }


    public KafkaProducer setConfigFile(String producerPropsFile) {
        this.PROPS_FILE = producerPropsFile;
        return this;
    }

    public int itemsProduced() {
        return this.ITEMS_PRODUCED;
    }

    public KafkaProducer initialize() {
        PropertiesConfiguration propsConfig = readProperties();

        /** Assign topicName to string variable */
        TOPIC = propsConfig.getString("topic");

        /** Get instance of kafka producer */
        this.PRODUCER = new org.apache.kafka.clients.producer.KafkaProducer(getKafkaProducerProps(propsConfig));
        return this;
    }

    /**
     * close producer instance
     */
    public void close() {
        this.PRODUCER.close();
    }


    /**
     * Description: void producer method
     */
    public void produce(String record) {
        this.ITEMS_PRODUCED++;
        this.PRODUCER.send(new ProducerRecord<String, String>(TOPIC, record));
    }


    /**
     * - Set of kafka producer props
     *
     * @param propsConfig - apache props configurations object
     * @return - Properties object
     */
    private static Properties getKafkaProducerProps(PropertiesConfiguration propsConfig) {
        // create instance for properties to access PRODUCER configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", propsConfig.getString("host") + ":" + propsConfig.getString("port"));

        //Set acknowledgements for PRODUCER requests.
        props.put("acks", "all");
        props.put("retries", 0);    //If the request fails, the PRODUCER can automatically retry,
        props.put("batch.size", 16384); //Specify buffer size in config
        props.put("linger.ms", 1);  //Reduce the no of requests less than 0

        //The buffer.memory controls the total amount of memory available to the PRODUCER for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }


    private PropertiesConfiguration readProperties() {
        String configFile = this.CONFIG_DIR + "/" + this.PROPS_FILE;

        /**
         * Build props config
         */
        PropertiesConfiguration propsConfig = null;
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(configFile);
            return (PropertiesConfiguration) config.interpolatedConfiguration();
        } catch (ConfigurationException e) {
            LOG.warn("Could NOT start, as configurations were not read successfully.");
            LOG.error("ConfigurationException:", e);
            System.exit(0);
        }


        return propsConfig;
    }


}
