package com.ahiel.simple.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author Ahielg
 * @date 20/12/2020
 */
public class BankBalanceExactlyOnceApp {

    public static final String INPUT_TOPIC = "bank-transactions";
    public static final String OUTPUT_TOPIC = "bank-balance-exactly-once";

    // json Serde
    static final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());

    static void createWordCountStream(final StreamsBuilder builder) {
        KStream<String, JsonNode> bankTransactions = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), jsonSerde));

        JsonNode initBalance = BankBalanceExactlyOnceApp.newRecord();
        KTable<String, JsonNode> counts = bankTransactions
                .groupByKey()
                //.groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(() -> initBalance,
                        (aggKey, newValue, aggValue) -> merge(aggValue, newValue),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("merge-bank-amount")
//                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        // need to override value serde to Long type
        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), jsonSerde));
    }

    private static JsonNode merge(JsonNode balance, JsonNode transaction) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("balance", balance.get("count").asInt() + transaction.get("amount").asInt());
        long max = Math.max(toEpocMillis(balance.get("time")), toEpocMillis(transaction.get("time")));
        newBalance.put("time", Instant.ofEpochMilli(max).toString());

        return newBalance;
    }

    private static long toEpocMillis(JsonNode time) {
        return Instant.parse(time.asText()).toEpochMilli();
    }


    static JsonNode newRecord() {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        transaction.put("count", 0);
        transaction.put("balance", 0);
        transaction.put("time", Instant.ofEpochMilli(0).toString());
        return transaction;
    }

    public static void main(String[] args) {
        //noinspection DuplicatedCode
        Properties config = getProps();

        StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        final CountDownLatch latch = new CountDownLatch(1);
        // shutdown hook to correctly close the streams application

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-bank-shutdown-hook") {
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
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "bank-application");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");/*
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());*/

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        return config;
    }
}
