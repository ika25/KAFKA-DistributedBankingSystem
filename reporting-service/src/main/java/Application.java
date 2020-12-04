import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Application {
	
	 private final static String TOPICS = "valid-transactions,suspicious-transactions";  
	 private final static String BOOTSTRAP_SERVERS =  "localhost:9092,localhost:9093,localhost:9094";
	 //private final static String BOOTSTRAP_SERVERS =  "localhost:9092";
	 

    public static void main(String[] args) {
    	Application application =new Application();  
    	KafkaConsumer<String,Transaction> kafkaConsumer =(KafkaConsumer<String, Transaction>) createKafkaConsumer( BOOTSTRAP_SERVERS,  "ReportingService");
    	kafkaConsumer.subscribe(Arrays.asList("valid-transactions","suspicious-transactions"));
    	//kafkaConsumer.subscribe(Arrays.asList("suspicious-transactions"));
    	consumeMessages(Arrays.asList(TOPICS), kafkaConsumer);
    	
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
    	
    	while(true) {
    		ConsumerRecords<String,Transaction> records= kafkaConsumer.poll(Duration.ofMillis(100)); 
    		   for(ConsumerRecord<String,Transaction> txnRecord: records){ 
    			   recordTransactionForReporting(txnRecord.topic(),txnRecord.value());
               }  
    	}
       
    }

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

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
    	 System.out.println(topic+"- "+transaction);
    }

}
