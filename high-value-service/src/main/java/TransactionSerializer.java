 
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TransactionSerializer implements Serializer { 

@Override
public byte[] serialize(String topic, Object data) {
	 byte[] retVal = null;
	    ObjectMapper objectMapper = new ObjectMapper();
	    try {
	      retVal = objectMapper.writeValueAsString(data).getBytes();
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    return retVal;
}

public void close() {
	  
}

}