import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by ramdurga on 2/8/15.
 */
public class TestContext {
  public static void main(String[] args) {
    ApplicationContext context =
        new ClassPathXmlApplicationContext("embedded-kafka.xml");
    System.out.println("Started Kafka cluster");
    System.out.println("### Embedded Kafka cluster broker list: " + ((EmbeddedKafkaCluster)(context.getBean("embeddedKafka"))).getBrokerList());
  }
}
