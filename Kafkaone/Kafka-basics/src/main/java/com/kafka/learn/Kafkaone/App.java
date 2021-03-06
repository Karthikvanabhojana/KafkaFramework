package com.kafka.learn.Kafkaone;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
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

//create producer record
ProducerRecord<String, String> record=new ProducerRecord<String, String>("first_topic","hello world");
//send data--asynchronous
producer.send(record);
producer.flush();
producer.close();
    }
}
