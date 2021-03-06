package com.kafka.learn.kafkatwo;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {
	
	String consumerKey="";
	String consumerSecret="";
	String token="";
	String secret="";
    List<String> terms = Lists.newArrayList("bitcoin","politics","cricket","india");
	Logger logger=LoggerFactory.getLogger(TwitterProducer.class.getName());
	public TwitterProducer() {
		
	}
	
	

	public static void main(String[] args) {
		
		new TwitterProducer().run();
		
	}
	public void run() {
		logger.info("Set Up");
		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		final KafkaProducer<String, String> producer= createKafkaProducer();
		//create twitter client
		final Client client= createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		
				//create a kafka  producer
				
				//loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Stopping application...");
			logger.info("Stopping twitter...");

			client.stop();
			logger.info("Stopping producer...");

			producer.close();
			logger.info("Done");

		}));
		while (!client.isDone()) {
		  String msg = null;
		try {
			msg = msgQueue.poll(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			client.stop();
		}
		  if(msg!= null) {
		 logger.info(msg);
		 producer.send(new ProducerRecord<String, String>("twitter_tweets",null, msg), new Callback() {
			
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if(exception!=null) {
					logger.error("Something bad Happened ",exception);
				}
			}
		});
		}
		}
		logger.info("End of appplication");
	
	}

public Client createTwitterClient(BlockingQueue<String> msgQueue) {
	
	/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
	Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
	StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
	// Optional: set up some followings and track terms
//	List<Long> followings = Lists.newArrayList(1234L, 566788L);
//	hosebirdEndpoint.followings(followings);
	hosebirdEndpoint.trackTerms(terms);

	// These secrets should be read from a config file
	Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
	
	
	
	ClientBuilder builder = new ClientBuilder()
			  .name("Hosebird-Client-01")                              // optional: mainly for the logs
			  .hosts(hosebirdHosts)
			  .authentication(hosebirdAuth)
			  .endpoint(hosebirdEndpoint)
			  .processor(new StringDelimitedProcessor(msgQueue));

			Client hosebirdClient = builder.build();
			return hosebirdClient;
			
}

public KafkaProducer<String, String> createKafkaProducer() {
	String bootstrapServers="127.0.0.1:9092";
Properties properties=new Properties();

//create producer properties
//properties.setProperty("bootstrap.servers", bootstrapServers);
//properties.setProperty("key.serializer", StringSerializer.class.getName());
//properties.setProperty("value.serializer", StringSerializer.class.getName());
properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


//safe producer
properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

//high throughput

properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


//create producer
KafkaProducer<String, String> producer=new KafkaProducer<String, String>(properties);
return producer;

}
}
