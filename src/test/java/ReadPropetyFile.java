import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by ramdurga on 2/8/15.
 */
public class ReadPropetyFile {
  public static void main(String[] args) {
    Properties prop = new Properties();
    InputStream input = null;

    try {
      System.out.println("Creating topics");
      input =EmbeddedKafkaCluster.class.getClassLoader().getResourceAsStream("topic.properties");

      prop.load(input);
      for(String key : prop.stringPropertyNames()) {
        String value = prop.getProperty(key);
        System.out.println("Topic created "+key);
      }

    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
