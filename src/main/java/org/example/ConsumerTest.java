package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import java.util.Properties;



public class ConsumerTest {
    public static void main(String[] args) {
        //카프카 서버
        String bootstrapServers = "localhost:9092";
        //컨슈머 그룹 아이디
        String groupId = "my-group";
        //토픽명
        String topic = "Org";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //컨슈머 구독
        consumer.subscribe(Collections.singleton(topic));

        while (true) {
            //consumer 객체를 통해서 poll()함수 실행
            ConsumerRecords<String, String> records = consumer.poll(300);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
