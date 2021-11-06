package com.kafka.learn.Kafkaone;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumerassignandseek {public static void main(String[] args) {

    Logger logger = LoggerFactory.getLogger(Consumerassignandseek.class.getName());

    String bootstrapServers = "localhost:9092";
//    String groupId = "my-fifth-application";
	String topic="first_topic";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // subscribe consumer to our topic(s)
//    consumer.subscribe(Arrays.asList(topic));
    
    //assign and seek are mostly used to replay data or fetch a specific message
    
    //assign
TopicPartition partitiontoReadfrom=new TopicPartition(topic, 0);
long offsettoReadFrom=15L;
consumer.assign(Arrays.asList(partitiontoReadfrom));

//seek
consumer.seek(partitiontoReadfrom, offsettoReadFrom);

int numberofmessage=5;
boolean keepOnReading=true;
int numberofmessagesread=0;
    // poll for new data


    while(true){
        ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

        for (ConsumerRecord<String, String> record : records){
            logger.info("Key: " + record.key() + ", Value: " + record.value());
            logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            if(numberofmessagesread>=numberofmessage) {
            	keepOnReading=false;
            	break;//exit of loop
            	
            }
        }
    }

}
}
