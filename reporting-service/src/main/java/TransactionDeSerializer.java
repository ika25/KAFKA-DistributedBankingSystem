import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionDeSerializer implements Deserializer { 
 
  @Override
  public Transaction deserialize(String arg0, byte[] arg1) {
    ObjectMapper mapper = new ObjectMapper();
    Transaction transaction = null;
    try {
      transaction = mapper.readValue(arg1, Transaction.class);
    } catch (Exception e) {

      e.printStackTrace();
    }
    return transaction;
  }

}