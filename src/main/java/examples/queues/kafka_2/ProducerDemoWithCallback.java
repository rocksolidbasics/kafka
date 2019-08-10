package examples.queues.kafka_2;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	
	private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
	
    public static void main( String[] args ) {
    	String bootstrapServers ="192.168.26.128:9092";
    	
    	//Properties
    	Properties props = new Properties();
    	//props.setProperty("bootstrap.server", bootstrapServers);
    	props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    	props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    	
    	//Create a producer
    	KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
    	
    	for(int i=0; i<10; i++) {
    		//Create the key
    		final String key = "id_" + i;
	    	//Send data - Asynchronous send, hence we need to do flush and send
    		//Create a producer record
    		ProducerRecord<String, String> record = 
    				new ProducerRecord<String, String>("code_topic_1", key, "Hello world " + i  +"!");

    		producer.send(record, new Callback() {
	
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						logger.info("Received new metadata for " + key + "\n"
						+ ", Topic: " + metadata.topic() 
						+ ", Partition: " + metadata.partition());
						//+ ", Offset: " + metadata.offset() 
						//+ ", Timestamp: " + metadata.timestamp());
					}
				}
	    		
	    	});
    	}
    	
    	producer.flush();
    	producer.close();
    	
    }
}
