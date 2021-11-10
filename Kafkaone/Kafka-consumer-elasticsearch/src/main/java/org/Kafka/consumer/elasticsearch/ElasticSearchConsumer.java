package org.Kafka.consumer.elasticsearch;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
public class ElasticSearchConsumer {
	  public static RestHighLevelClient createClient(){

	        String hostname = ""; // localhost or bonsai url
	        String username = ""; // needed only for bonsai
	        String password = ""; // needed only for bonsai

	        // credentials provider help supply username and password
	        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	        credentialsProvider.setCredentials(AuthScope.ANY,
	                new UsernamePasswordCredentials(username, password));
RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                   @Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						// TODO Auto-generated method stub
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
                });

	        RestHighLevelClient client = new RestHighLevelClient(builder);
	        return client;
	    }
	  
	  
	  public static KafkaConsumer<String, String> createConsumer(String topic) {
		  String bootstrapServers = "localhost:9092";
		    String groupId = "kafka_demo-elasticsearch";

		    // create consumer configs
		    Properties properties = new Properties();
		    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");



		    // create consumer
		    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		    consumer.subscribe(Arrays.asList(topic));
			return consumer;
	  }
public static void main(String[] args) throws IOException {
	Logger logger=LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

RestHighLevelClient client =createClient();
	
	
	
	
	KafkaConsumer<String, String> consumer=createConsumer("twitter_tweets");
	
	 while(true){
	        ConsumerRecords<String, String> records =
	                consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
	        Integer record_count=records.count();
logger.info("Receiver "+record_count +" records");
BulkRequest bulkRequest=new BulkRequest();
	        for (ConsumerRecord<String, String> record : records){
	        	
	        	try {
	        		String jsonstring=record.value();
		        	//generic id
//		        	String id=record.topic() +"_"+record.partition()+ "_"+record.offset();
		        	//twitter feed specific ID
		    	    String id= extractIDfromTweet(record.value());

					IndexRequest indexRequest=new IndexRequest("twitter", "tweets",id  //for Idempotence
		        			).source(jsonstring, XContentType.JSON);
					
					
	bulkRequest.add(indexRequest); //we add to our bulk request
				} catch (NullPointerException e) {
					// TODO: handle exception
					logger.warn("skipping bad data : " +record.value());
				}
	        	



//	        	IndexResponse indexResponse=client.index(indexRequest, RequestOptions.DEFAULT);
////	        	String id=indexResponse.getId();
//	        	logger.info(indexResponse.getId());
//	        	try {
//					Thread.sleep(10);
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}

//	            logger.info("Key: " + record.key() + ", Value: " + record.value());
//	            logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());



	        }
	        if(record_count>0) {
	        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
	        logger.info("Commiting the offsets...");
	        consumer.commitSync();
	        
	        logger.info("Requests are commited");
	        try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
			}
}
	 }
//		client.close();
}

 private static JsonParser jsonParser=new JsonParser();
private static String extractIDfromTweet(String tweetJson) {
	// TODO Auto-generated method stub
	return jsonParser.parse(tweetJson)
	.getAsJsonObject()
	.get("id_str")
	.getAsString();
	
}
}
