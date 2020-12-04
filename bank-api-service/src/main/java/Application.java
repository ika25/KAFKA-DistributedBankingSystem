import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
  

/**
 * Banking API Service
 */
public class Application {
	
	/**
	 * Below are the topics the banking-api-service publishes the messages	 */
	 private final static String VALID_TXN_TOPIC = "valid-transactions";
	 private final static String SUSPICIOUS_TXN_TOPIC = "suspicious-transactions";
	 private final static String HIGHVALUE_TXN_TOPIC = "highvalue-transactions"; 
	 
	 /**
	  * These are the kafka brokers running on the mentioned port in localhost
	  */
	 private final static String BOOTSTRAP_SERVERS =  "localhost:9092,localhost:9093,localhost:9094";
	// private final static String BOOTSTRAP_SERVERS =  "localhost:9092";

	 
	 /**
	  * Starting point of the applications. This methods reads the transactions from text file
	  * and are used to send to the respective topics for the cosumers 
	  * @param args
	  * @throws ExecutionException
	  * @throws InterruptedException
	  */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase(); 
        Application application=new Application();
        application.processTransactions(incomingTransactionsReader, customerAddressDatabase, application.createKafkaProducer(BOOTSTRAP_SERVERS));

      }
    
    
    /**
     * This method does the actual work of publishing into topics based on the logic defined
     * 
     * If address does not match with the location of the transaction then treated as Suspicious trnsactions and
     * posted into suspicious-transactions queue.
     * 
     * 
     * 
     * The transactions are parsed
     * @param incomingTransactionsReader
     * @param customerAddressDatabase
     * @param kafkaProducer
     * @throws ExecutionException
     * @throws InterruptedException
     */

   public void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException {
    while(incomingTransactionsReader.hasNext()) { 
         	Transaction transaction= incomingTransactionsReader.next();
         	String txnUser = transaction.getUser();
         	String txnLocation=transaction.getTransactionLocation();
         	String userResidence = customerAddressDatabase.getUserResidence(txnUser);
         	Transaction txn=new Transaction(txnUser,transaction.getAmount(),transaction.getTransactionLocation());
         	 
         	ProducerRecord<String,Transaction> producerRecord =null;
         	 //validating the userresidence with the location of transaction
         	if(userResidence!=null&&txnLocation!=null&&txnLocation.equalsIgnoreCase(userResidence)) { 
         		//producerRecord = new ProducerRecord(VALID_TXN_TOPIC,txn); 
         		producerRecord =new ProducerRecord<String, Transaction>(VALID_TXN_TOPIC, userResidence, txn);
         	}else {  
         		//if location and user residence are different treat it as suspicious and move it to suspicious topic
         		producerRecord =new ProducerRecord<String, Transaction>(SUSPICIOUS_TXN_TOPIC, userResidence, txn);
         	} 
         	
         	//if transaction amount is greater than 0 consider as suspicious transaction
         	double amount = txn.getAmount();
         	if(amount>10000) {
         		System.out.println(" Publishing txn amount > 10000 "+transaction);
         		producerRecord =new ProducerRecord<String, Transaction>(HIGHVALUE_TXN_TOPIC, userResidence, txn); 
         	}
         	 kafkaProducer.send(producerRecord); 
    	 } 
   
    	 kafkaProducer.flush();
  		 kafkaProducer.close();  
    }

    
   /**
    * This method creates kafka producer using the defined properties.
    * 
    * @param bootstrapServers
    * @return
    */
   
   public Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
	Properties kafkaProperties = new Properties();
	kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
	kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
	kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TransactionSerializer.class.getName());
	KafkaProducer<String,Transaction> kafkaProducer = new KafkaProducer<String,Transaction>(kafkaProperties); 
	return kafkaProducer;
 
   }

}
