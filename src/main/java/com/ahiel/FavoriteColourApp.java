package com.ahiel;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Ahielg
 * @date 15/12/2020
 */
public class FavoriteColourApp {
    public static final String INPUT_TOPIC = "favourite-colour-input";
    public static final String OUTPUT_TOPIC = "favourite-colour-output";
    public static final String INTERMEDIATE_TOPIC = "user-keys-and-colours";
    private static final boolean devMode = true;
    private static final List<String> AVAILABLE_COLOURS = Arrays.asList("green", "blue", "red");

    static void createWordCountStream(final StreamsBuilder builder) {
        //KStream<String, String> textLines =
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((k, v) -> v.contains(","))
                .mapValues(value -> value.toLowerCase(Locale.getDefault()))
                .selectKey((k, v) -> v.split(",")[0])
                .mapValues((k, v) -> v.split(",")[1])
                .filter((user, colour) -> AVAILABLE_COLOURS.contains(colour))
                .to(INTERMEDIATE_TOPIC);

        KTable<String, String> table = builder.table(INTERMEDIATE_TOPIC);

        KTable<String, Long> favoriteColour = table
                .groupBy((k, v) -> new KeyValue<>(v, v))
                //.groupBy((k, v) -> KeyValue.pair(v, v))
                .count(Named.as("CountByColours"));

        favoriteColour.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
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
            if (devMode) {
                streams.cleanUp();
            }

            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }

    private static Properties getProps() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        //config.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "30000");

        if (devMode) {
            config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        }
        return config;
    }
}
