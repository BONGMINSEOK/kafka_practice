package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerTest {
    public static void main(String[] args) {
        // Kafka 브로커 서버의 주소와 포트를 설정합니다.
        String bootstrapServers = "localhost:9092";

        // Producer 설정을 생성합니다.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 인스턴스를 생성합니다.
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        // ProducerRecord를 생성하여 메시지를 보냅니다.
        ProducerRecord<String, String> record = new ProducerRecord<>("Org", "A1", '{Val:"123"}');

        // 메시지를 전송합니다.
        producer.send(record);
        // 프로듀서를 닫습니다.
        producer.close();
    }
}
