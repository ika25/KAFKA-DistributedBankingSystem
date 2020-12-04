 
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * publisher serializes the messages so that the content and format is not 
 * lost during transition
 * @author Admin
 *
 */

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