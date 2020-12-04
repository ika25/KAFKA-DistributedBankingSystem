import org.apache.kafka.clients.consumer.*; 
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;


/**
 * This class handles the suspicious transactions. Suspicious transactions are those which
 *have a different region for transactions and registered location
 * @author Admin
 *
 */
public class Application {
	 private final static String BOOTSTRAP_SERVERS =  "localhost:9092,localhost:9093,localhost:9094";
	  // private final static String BOOTSTRAP_SERVERS =  "localhost:9092";
	   private final static String SUSPICIOUS_TXN_TOPIC = "suspicious-transactions"; 

	   
	   /**
	    * starting point of the application. 
	    * @param args
	    */
    public static void main(String[] args) {
    
    	Application application =new Application();  
    	KafkaConsumer<String,Transaction> kafkaConsumer =(KafkaConsumer<String, Transaction>) createKafkaConsumer( BOOTSTRAP_SERVERS,  "UserNotificationService");
    	
    	application.consumeMessages(SUSPICIOUS_TXN_TOPIC, kafkaConsumer);
    	
    	
    	
    }

    
    /**
     * consumes the messages from the suspicious-transactions topic
     * @param topic
     * @param kafkaConsumer
     */
    public static void consumeMessages(String topic, Consumer<String, Transaction> kafkaConsumer) {
    	while(true) {
    		ConsumerRecords<String,Transaction> records= kafkaConsumer.poll(Duration.ofMillis(100)); 
    		   for(ConsumerRecord<String,Transaction> txnRrecord: records){ 
    			   sendUserNotification(txnRrecord.value());
               }  
    	}
    }

    
    /**
     * creates the consumer using the properties passed
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

     
     /**
      * sends user notification to the console
      * @param transaction
      */
    private static void sendUserNotification(Transaction transaction) {
    	 System.out.println(transaction); 
		  
    }
    
    
    
 

}
