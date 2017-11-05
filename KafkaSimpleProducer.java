import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaSimpleProducer {

	public static void main(String[] args) {
		  String topicName = "credit_card";
		  String key = "Key123";
		  String value = "Value-12323";
		  Properties props = new Properties();
		  
		  try {
			   props.load(new FileInputStream("kafka.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	     
		        
	      Producer<String, String> producer = new KafkaProducer <>(props);
	      
	      Map<String,String> valueMap=new HashMap<>();
	      valueMap.put("a", "1");
	      valueMap.put("a", "12");
	      valueMap.put("a", "13");
	      valueMap.put("a", "14");
	      valueMap.put("a", "15");
	      valueMap.put("a", "16");
	      valueMap.put("a", "17");
	      valueMap.put("a", "19");
	      
//		  valueMap.forEach((k,v) -> 
//		  { ProducerRecord<String, String> record=new ProducerRecord<>(topicName,k,v); 
//		  			producer.send(record);
//		  			producer.close();
//		  } 
//				  );
		  
	      for (int i=0 ; i<10 ; i++)
	          producer.send(new ProducerRecord<>(topicName,"SSP"+i,"500"+i));
	      
	      ProducerRecord<String, String> record=null;
		  for(Map.Entry<String,String> e:valueMap.entrySet())
		  {
			  record=new ProducerRecord<>(topicName,e.getKey(),e.getValue());
			  producer.send(record);
			  
		  }
		  
		  for (Map.Entry<String, String> entry : valueMap.entrySet()) {
				System.out.println("Item : " + entry.getKey() + " Count : " + entry.getValue());
			}
		  
		  producer.close();
		  
		  
		  System.out.println("Producer Completed.");
	}

}
