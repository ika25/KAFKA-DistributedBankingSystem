import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;


/**
 * 
 * @author Admin
 * Account Manager consumer listens on valid-transactions topic.
 * When Banking-api-service publishes messages, Account manager consumer consumes the messages and 
 * prints the received transactions
 *
 */
public class Application {
	//All the borkers are running in localhost on port 9092, 9093, 9094
	  private final static String BOOTSTRAP_SERVERS =  "localhost:9092,localhost:9093,localhost:9094";
	 //  private final static String BOOTSTRAP_SERVERS =  "localhost:9092";
	  
	  //This is the topic  account-manager is listening
	   private final static String SUSPICIOUS_TXN_TOPIC = "valid-transactions"; 

	   
	   //Application starts with main method when executed with jar
    public static void main(String[] args) {
    	Application application =new Application();  
    	KafkaConsumer<String,Transaction> kafkaConsumer =(KafkaConsumer<String, Transaction>) createKafkaConsumer( BOOTSTRAP_SERVERS,  "AccountManagerService");
    	
    	application.consumeMessages(SUSPICIOUS_TXN_TOPIC, kafkaConsumer);
    }

    
    /**
     * This is infinite loop able to grab the messages when posted by bank-api-service
     * @param topic
     * @param kafkaConsumer
     */
    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
    	while(true) {
    		ConsumerRecords<String,Transaction> records= kafkaConsumer.poll(Duration.ofMillis(100)); 
    		   for(ConsumerRecord<String,Transaction> txnRrecord: records){ 
    			   approveTransaction(txnRrecord.value());
               }  
    	}
    }

    
    /**
     * This method creates the KafkaConsumer using the properties populated.
     * @param bootstrapServers
     * @param consumerGroup
     * @return
     */
    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
    	Properties kafkaProperties = new Properties();
     	kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
     	kafkaProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
     	kafkaProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TransactionDeSerializer.class.getName());
     	kafkaProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); 
     	kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroup);  
     	KafkaConsumer<String,Transaction> kafkaConsumer = new KafkaConsumer<String,Transaction>(kafkaProperties); 
     	kafkaConsumer.subscribe(Arrays.asList(SUSPICIOUS_TXN_TOPIC)); 
     	return kafkaConsumer;
     }

    private static void approveTransaction(Transaction transaction) {
    	 System.out.println(transaction); 
    }

}
