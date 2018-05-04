package org.muks.kafka;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


public class SimpleProducer {
    private static Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
    private static boolean KEEP_PRODUCING = true;
    private static Producer<String, String> PRODUCER = null;
    private static String TOPIC;
    private static int LINES_PRODUCED = 0;

    public static void main(String[] args) {
        String configDir = args[0];
        String configFile = args[1];


        /** for kill -9 */
        Runtime.getRuntime().addShutdownHook(new ProcessorHook());

        PropertiesConfiguration propsConfig = readProperties(configDir + "/" + configFile);

        /** Assign topicName to string variable */
        TOPIC = propsConfig.getString("topic");

        /** Get instance of kafka producer */
        PRODUCER = new KafkaProducer<>(getProperties(propsConfig));


        while (KEEP_PRODUCING) {
            /** Keep reading sample data file and publish it over kafka */
            File sampleDataFile = new File(configDir + "/" + propsConfig.getString("data.file"));
            publishFileContent(sampleDataFile);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        LOG.info("Message sent successfully");
        PRODUCER.close();
    }


    private static Properties getProperties(PropertiesConfiguration propsConfig) {
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

    private static void publishFileContent(File sampleDataFile) {

        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(sampleDataFile));
            String readLine = "";

            while ((readLine = bufferedReader.readLine()) != null) {
                LINES_PRODUCED++;
                PRODUCER.send(new ProducerRecord<String, String>(TOPIC, readLine));
            }

        } catch (IOException e) {
            e.printStackTrace();

        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static PropertiesConfiguration readProperties(String configFile) {
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

    public static class ProcessorHook extends Thread {
        @Override
        public void run() {
            KEEP_PRODUCING = false;
            LOG.info("Done producing " + LINES_PRODUCED + " lines of the sample content.");
            LOG.info("Shutdown hook() called, shutting down kafka PRODUCER now.");
        }
    }




//    /**
//     * Utility for parsing command line flags, using apache commons CLI. Stop JVM, if exception.
//     */
//    private void parseCommandLine(String[] args) {
//        LOG.info("Process start time: " );
//
//        String hlpTxt
//                = "java -cp <jar-file> " + VarianceReportsEventCounts.class.getName()
//                + " -configDir <variance-manager.properties dir abs path> "
//                + " -configFile <variance-manager.properties file path> "
//                + " -cleanup <true|false> ";
//
//
//        ParseCLI parser = new ParseCLI(hlpTxt);
//
//        List<ParserOptionEntity> parserOptionEntityList = new ArrayList<>();
//        parserOptionEntityList.add(new ParserOptionEntity("configDir", "Input configurations dir.", true));
//        parserOptionEntityList.add(new ParserOptionEntity("configFile", "Input configurations file.", true));
//        parserOptionEntityList.add(new ParserOptionEntity("cleanup", "Clean up data/ws output dir.", true));
//
//        ParserOptions parserOptions = new ParserOptions(parserOptionEntityList);
//        String configFile = null;
//        String configDir = null;
//
//        try {
//            CommandLine commandline = parser.getCommandLine(args, 2, parserOptions);
//
//            if (commandline.hasOption("configDir")) {
//                configDir = commandline.getOptionValue("configDir");
//                System.setProperty("configDir", configDir);
//            }
//
//            if (commandline.hasOption("configFile")) {
//                configFile = commandline.getOptionValue("configFile");
//                System.setProperty("configFile", configFile);
//            }
//
//            if (commandline.hasOption("cleanup")) {
//                CleanupRequired = Boolean.parseBoolean(commandline.getOptionValue("cleanup"));
//            }
//
//            configFile = configDir + "/" + configFile;
//            if (!new File(configFile).exists()) {
//                LOG.warn("Config file = " + configFile + " does NOT exists.");
//                System.exit(0);
//            }
//
//        } catch (ParseException e) {
//            e.printStackTrace();
//            System.exit(0);
//
//        }
//    }
}
