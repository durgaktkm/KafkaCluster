/**
 * Created by ramdurga on 11/18/14.
 */

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.net.URL;
import java.util.Properties;

public class SimpleProducer {

  private static Producer<Integer, String> producer;
  private final Properties props = new Properties();

  public SimpleProducer() {

    props.put("metadata.broker.list", "localhost:62983,localhost:62984");
   //props.put("metadata.broker.list", "ec2-54-174-87-75.compute-1.amazonaws.com:9092");
    //props.put("metadata.broker.list", "localhost:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    producer = new Producer<Integer, String>(new
        ProducerConfig(props));
  }

  public static void main(String[] args) throws IOException {
    SimpleProducer sp = new SimpleProducer();
    String topic = "deviceData";
    URL url = Resources.getResource("prepare.json");
    String messageStr = Resources.toString(url, Charsets.UTF_8);
    //String messageStr = "Hello there";
    KeyedMessage<Integer, String> data = new KeyedMessage<Integer,
        String>(topic, messageStr);
    for(int i=0;i<1;i++){
      producer.send(data);
    }
    System.out.println("Finished sending data");;
    producer.close();
  }
}


