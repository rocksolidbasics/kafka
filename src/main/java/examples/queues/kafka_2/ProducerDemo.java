package examples.queues.kafka_2;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo 
{
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
    	
    	//Create a producer record
    	ProducerRecord<String, String> record = 
    			new ProducerRecord<String, String>("code_topic_1", "Hello world!");
    	
    	//Send data - Asynchronous send, hence we need to do flush and send
    	producer.send(record);
    	producer.flush();
    	producer.close();
    	
    }
}
