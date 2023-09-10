package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.connector.kafka.source.*;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import java.util.Properties;
import com.google.gson.Gson;


public class Main {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("pkc-6ojv2.us-west4.gcp.confluent.cloud:9092")
                .setTopics("topic_0")
//        	.setStartingOffsets(StartupOptions.earliest())
//        	.setValueOnlyDeserializer(new KafkaDeserializationSchemaWrapper<>(new SimpleStringSchema()))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "PLAIN")
                .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='SCSIOQFOWUZKH5MN' password='wYuE7hiMS84L4sjJKp+aGlFMr24FICt4vWZBPI+lxnvTqsjbGIo8PDwFslzLGTiC';")
                .build();

        DataStream<String> kafkaData = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );


        DataStream<Order> orderDataStream = kafkaData.map(new JSONStringToOrderMapper());


        DataStream<Tuple2<String, Integer>> counts =  orderDataStream
                        .map(new CountItemsPerItemID())         // split up the lines in pairs (2-tuples) containing: tuple2 {(name,1)...}
                        .keyBy(0).sum(1);            // group by the tuple field "0" and sum up tuple field "1"

        counts.print();

        DataStream<OutputData> jsonDataStream = counts.map(new MapFunction<Tuple2<String, Integer>, OutputData>() {
            @Override
            public OutputData map(Tuple2<String, Integer> value) throws Exception {
                OutputData myData = new OutputData();
                myData.setKey(value.f0);
                myData.setValue(value.f1);
                return myData;
            }
        });

        DataStream<String> jsonStringStream = jsonDataStream.map(new MapFunction<OutputData, String>() {
            @Override
            public String map(OutputData value) throws Exception {
                Gson gson = new Gson();
                return gson.toJson(value);
            }
        });


        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092");
        producerProps.setProperty("security.protocol", "SASL_SSL");
        producerProps.setProperty("sasl.mechanism", "PLAIN");
        producerProps.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='SCSIOQFOWUZKH5MN' password='wYuE7hiMS84L4sjJKp+aGlFMr24FICt4vWZBPI+lxnvTqsjbGIo8PDwFslzLGTiC';");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "topic_1", // Replace with your Kafka topic name
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), // Serialization schema for the Kafka topic
                producerProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        jsonStringStream.addSink(kafkaProducer);
        env.execute("Kafka Example");

    }

    // Custom MapFunction to count items per 'itemid'
    public static class CountItemsPerItemID implements MapFunction<Order, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(Order order) {
            return new Tuple2<>(order.getItemid(), 1);
        }
    }

}
