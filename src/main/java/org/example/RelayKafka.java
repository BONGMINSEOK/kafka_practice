package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class RelayKafka {
    public static void main(String[] args) {
        //Producer 생성
        String bootstrapServers = "localhost:9092";
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        //Consumer 생성
        String groupId = "my-group";
        String topic = "Org";
        Properties consumerProperty = new Properties();
        consumerProperty.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperty.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperty.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperty.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperty);

        // 과정 4: 해당 과정을 무한루프를 통해서 구현한다.
        while(true){
            //과정 1: 발전소 RelayServer에서 ConsumerRecord를 poll 한다.
            ConsumerRecords<String, String> records = consumer.poll(300);
            for (ConsumerRecord<String, String> record : records) {
                // 과정 2: 읽어온 ConsumerRecord를 ProducerRecord로 만들어준다.
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("Org",record.key(), record.value());
                // 과정 3: 전력연구원 RelayServer에 ProducerRecord를 Send 한다.
                producer.send(producerRecord);
            }
        }

    }
}
