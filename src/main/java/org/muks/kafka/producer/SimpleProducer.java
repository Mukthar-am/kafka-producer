package org.muks.kafka.producer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.muks.kafka.cli.ParseCLI;
import org.muks.kafka.cli.ParserOptionEntity;
import org.muks.kafka.cli.ParserOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class SimpleProducer {
    private static Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);
    private static boolean KEEP_PRODUCING = true;
    private static Producer<String, String> PRODUCER = null;
    private static String TOPIC;
    private static int LINES_PRODUCED = 0;
    private static String CONFIG_DIR;
    private static String PROPS_FILE;
    private static Integer DELAY = 0;

    public static void main(String[] args) {
        parseCommandLine(args);

        /** for kill -9 */
        Runtime.getRuntime().addShutdownHook(new ProcessorHook());

        PropertiesConfiguration propsConfig = readProperties(PROPS_FILE);

        /** Assign topicName to string variable */
        TOPIC = propsConfig.getString("topic");

        /** Get instance of kafka producer */
        PRODUCER = new KafkaProducer<>(getProperties(propsConfig));


        File sampleDataFile = new File(CONFIG_DIR + "/" + propsConfig.getString("data.file"));

        if (!sampleDataFile.exists()) {
            LOG.error("File - %s, does NOT exists", sampleDataFile.getAbsolutePath());
            System.exit(0);
        }

        while (KEEP_PRODUCING) {
            /** Keep reading sample data file and publish it over kafka */
            publishFileContent(sampleDataFile);

            if (DELAY > 0) {
                System.out.println("Delaying .... ");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Not delaying ");
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




    /**
     * Utility for parsing command line flags, using apache commons CLI. Stop JVM, if exception.
     */
    private static void parseCommandLine(String[] args) {
        LOG.info("Process start time: " );

        String hlpTxt
                = "java -cp <jar-file> " + SimpleProducer.class.getName()
                + " -configDir <producer.properties dir abs path> "
                + " -configFile <producer.properties file path>"
                + " -delay <Interger in milliseconds>";


        ParseCLI parser = new ParseCLI(hlpTxt);

        List<ParserOptionEntity> parserOptionEntityList = new ArrayList<>();
        parserOptionEntityList.add(new ParserOptionEntity("configDir", "Input configurations dir.", true));
        parserOptionEntityList.add(new ParserOptionEntity("configFile", "Input configurations file.", true));
        parserOptionEntityList.add(new ParserOptionEntity("delay", "Input configurations file.", true));

        ParserOptions parserOptions = new ParserOptions(parserOptionEntityList);
        String configFile = null;

        try {
            CommandLine commandline = parser.getCommandLine(args, 3, parserOptions);

            if (commandline.hasOption("configDir"))
                CONFIG_DIR = commandline.getOptionValue("configDir");

            if (commandline.hasOption("configFile"))
                configFile = commandline.getOptionValue("configFile");

            if (commandline.hasOption("delay"))
                DELAY = Integer.valueOf(commandline.getOptionValue("delay"));


            PROPS_FILE = CONFIG_DIR + "/" + configFile;
            if (!new File(PROPS_FILE).exists()) {
                LOG.warn("Config file = %s does NOT exists", PROPS_FILE);
                System.exit(0);
            }

        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(0);

        }
    }
}
