package pl.com.sages.hadoop.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class KafkaConsumerExample {

    private static final Logger LOGGER = Logger.getLogger(KafkaConsumerExample.class);

    public static void main(String[] args) {

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group" + new Random(System.currentTimeMillis()).nextInt());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig);
        consumer.subscribe(Collections.singletonList("test"), new TestConsumerRebalanceListener());

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
            if (records.count() > 0) {
                LOGGER.info("Poll records: " + records.count());
            }

            consumer.commitSync();
        }

    }

}