package com.oracle.oci;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerAuthToken {

    static String bootstrapServers = "streaming.us-ashburn-1.oci.oraclecloud.com:9092"; // usually of the form
    // cell-1.streaming.<region>.oci.oraclecloud.com:9092
    // ;
    //static String tenancyName = "orasenatdpltintegration01";
    static String tenancyName = "windstreamoci";
    //static String username = "oracleidentitycloudservice/andrew.gregory@oracle.com";
    //static String username = "QRadar_Stream_Usr";
    static String username = "N9889247";
    //static String streamPoolId = "ocid1.streampool.oc1.iad.amaaaaaaytsgwayaqu64fqjs32sna5dwxwcqvgnpv3py6y4nvp4yomen26nq";
    // windstream below
    static String streamPoolId = "ocid1.streampool.oc1.iad.amaaaaaahwcsg7aatq6enm6eob5kpnifcpyelsaolzcgygmrfruyszde3jna";
    //static String authToken = "Mg0<:m:0PY7t#>R4B3Td"; // ag - integration01
    //static String authToken = "R7cAt4ZgnL+DQMummA_{"; // windstream actual
    static String authToken = "p50YoGNihn{y8T.{3l78"; // windstream AG
    static String streamOrKafkaTopicName = "QRadar-Stream"; // from step 2 of Prerequisites section
    static String consumerGroupName = "group-java";

    private static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", consumerGroupName);
        props.put("enable.auto.commit", "false");
        props.put("session.timeout.ms", "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("auto.offset.reset", "earliest");
        final String value = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + tenancyName + "/"
                + username + "/"
                + streamPoolId + "\" "
                + "password=\""
                + authToken + "\";";
        props.put("sasl.jaas.config", value);
        return props;
    }

    public static void main(String[] args) {
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(getKafkaProperties());
        ;
        consumer.subscribe(Collections.singletonList(streamOrKafkaTopicName));
        ConsumerRecords<Integer, String> records = consumer.poll(10000);

        System.out.println("size of records polled is " + records.count());
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(
                    "Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }

        consumer.commitSync();
        consumer.close();
    }
}
