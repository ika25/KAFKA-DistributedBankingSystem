import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
/**
 * This service consumes messages from highvalue_Transactions topic
 * Transactions which are greater than 10k value are posted to this queue.
 * @author Admin
 *
 */
public class Application {
	   private final static String BOOTSTRAP_SERVERS =  "localhost:9092,localhost:9093,localhost:9094";
	  // private final static String BOOTSTRAP_SERVERS =  "localhost:9092";
	   private final static String HIGHVALUE_TXN_TOPIC = "highvalue-transactions"; 

	   
   /**
    * Starting point of the application which would enable 
    * the application to listen the high value transaction messages.
    * @param args
    */
    public static void main(String[] args) {
    	Application application =new Application();  
    	KafkaConsumer<String,Transaction> kafkaConsumer =(KafkaConsumer<String, Transaction>) createKafkaConsumer( BOOTSTRAP_SERVERS,  "HighValueService");
      	kafkaConsumer.subscribe(Arrays.asList(HIGHVALUE_TXN_TOPIC)); 
    	application.consumeMessages(HIGHVALUE_TXN_TOPIC, kafkaConsumer);
    }

    
    /**
     * 
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
     * This method is used to consume the messages posted to highvalue_Transactions topic
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
     	return kafkaConsumer;
     }

    
    /**
     * prints to the console.
     * @param transaction
     */
    private static void approveTransaction(Transaction transaction) {
    	 System.out.println(transaction); 
    }

}
