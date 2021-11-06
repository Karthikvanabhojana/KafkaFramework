package com.kafka.learn.Kafkaone;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Appkeys {
public static void main(String[] args) throws InterruptedException, ExecutionException {
	
	
final Logger  logger =LoggerFactory.getLogger(Appkeys.class);
 String bootstrapServers="127.0.0.1:9092";
Properties properties=new Properties();

//create producer properties
//properties.setProperty("bootstrap.servers", bootstrapServers);
//properties.setProperty("key.serializer", StringSerializer.class.getName());
//properties.setProperty("value.serializer", StringSerializer.class.getName());
properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


//create producer
KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
for(int i=0;i<10;i++)
{
//create producer record
	String topic="first_topic";
	String value="hello worlds"+Integer.toString(i);
	String key="id_"+Integer.toString(i);
ProducerRecord<String, String> record=new ProducerRecord<String, String>(topic,key,value);
logger.info("Key:" +key);
//Key:id_0 Partition:1
//Key:id_1 Partition:0
//Key:id_2 Partition:2
//Key:id_3 Partition:0
//Key:id_4 Partition:2
//Key:id_5 Partition:2
//Key:id_6 Partition:0
//Key:id_7 Partition:2
//Key:id_8 Partition:1
//Key:id_9 Partition:2

//send data--asynchronous
producer.send(record,new Callback() {
	
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub//executes everytime record is sent or exception is thrown
		
		if(exception==null) {
		logger.info("Recieved new metadata.\n "+
		"Topic: "+metadata.topic()+ "\n"+
		"Partition: "+metadata.partition()+"\n"+
		"Offset: "+metadata.offset()+"\n"+
		"Timestamp: "+metadata.timestamp());	

}else {
			logger.error("Error While Producing" +exception);
		}
		
		
	}
}).get();//block the .send() to make it synchronous should not be done
}
producer.flush();
producer.close();
}
}
