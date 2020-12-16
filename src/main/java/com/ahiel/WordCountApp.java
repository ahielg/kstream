package com.ahiel;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Ahielg
 * @date 15/12/2020
 */
public class WordCountApp {

    public static final String INPUT_TOPIC = "word-count-input";
    public static final String OUTPUT_TOPIC = "word-count-output";

    static void createWordCountStream(final StreamsBuilder builder) {
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, Long> wordCounts = textLines.mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((k, v) -> v)
                .groupByKey()
                .count();


        // 7 - to in order to write the results back to kafka
        wordCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }


    public static void main(String[] args) {
        Properties config = getProps();

        StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        final CountDownLatch latch = new CountDownLatch(1);
        // shutdown hook to correctly close the streams application

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

    private static Properties getProps() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return config;
    }
}
