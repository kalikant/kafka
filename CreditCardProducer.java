import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class CreditCardProducer {

	public static void main(String[] args) {
		
		  String topicName = "credit_card";
		  String key = "Key1";
		  String value = "Value-1234";
		  Properties props = new Properties();
		  ProducerRecord<String, String> record= null;
		  
		  try {
			   props.load(new FileInputStream("kafka.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	     
		        
	      Producer<String, String> producer = new KafkaProducer <>(props);
		
	      for(int i=0;i<10;i++)
	      {
	    	  record = new ProducerRecord<>(topicName,key,value+i);
			  producer.send(record);
	      }
		 	
		  
	      producer.close();
		  
		  System.out.println("CreditCardProducer Completed.");

	}

}
