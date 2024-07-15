import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Date;

public class Consumer {
    private String topic = "chat";
    private String groupId = "my-group";
    private KafkaConsumer<String, String> consumer;
    private AtomicBoolean running = new AtomicBoolean(true);

    public Consumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void processEvents() {
        try {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Consumed message: \n" + "Key: " + record.key() + " Value: " + record.value() + " @ " + new Date(record.timestamp()));
                    System.out.println(record.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        running.set(false);
    }
}


/*
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import java.util.Date;

public class Consumer {
	String topic = "chat";
	String groupId = "my-group";
	KafkaConsumer<String, String> consumer;

	public Consumer() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		//props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(topic));
	}

	public void processEvents() {
		while (true) {
			
			// Poll to get partition information
			//consumer.poll(0);
			//List<TopicPartition> partitions = consumer.assignment().stream().collect(Collectors.toList());

			// Explicitly set offset to beginning
			//consumer.seekToBeginning(partitions);
			
			// Poll for messages
			ConsumerRecords<String, String> records = consumer.poll(1000);

			for (ConsumerRecord<String, String> record : records) {
				System.out.println("Consumed message: " + "\nKey: " + record.key() + " Value: " + record.value() + " @ " + new Date(record.timestamp()));
			}
		}
	}

}
*/