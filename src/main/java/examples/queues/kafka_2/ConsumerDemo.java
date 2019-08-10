package examples.queues.kafka_2;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
	
	public static void main(String[] args) {
		String bootServers = "192.168.26.128:9092";
		
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootServers);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "cons-grp-code-1");
		//Valid values - Earliest (Read from the beginning of the topic)
		//latest (Read only the latest), none - Will throw an error
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		//Create consumer
		KafkaConsumer<String, String> cons = new KafkaConsumer<String, String>(props);
		//Subscribe consumer
		cons.subscribe(Collections.singleton("code_topic_1"));
		//Poll for new data
		while(true) {
			ConsumerRecords<String, String> recs = cons.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> rec : recs) {
				logger.info("Key: " + rec.key() + ", value: " + rec.value() 
						+ ", partition: " + rec.partition());
			}
		}
	}
}
