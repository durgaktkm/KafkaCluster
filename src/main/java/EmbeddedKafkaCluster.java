import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.springframework.context.SmartLifecycle;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Created by ramdurga on 2/8/15.
 */
public class EmbeddedKafkaCluster implements SmartLifecycle {

  private final List<Integer> ports;
  private final String zkConnection;
  private final Properties baseProperties;

  private final String brokerList;

  private final List<KafkaServer> brokers;
  private final List<File> logDirs;

  private boolean isRunning=false;
  private EmbeddedZookeeper zookeeper;

  public EmbeddedKafkaCluster(String zkConnection) {
    this(zkConnection, new Properties());
  }

  public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties) {
    this(zkConnection, baseProperties, Collections.singletonList(-1));
  }
  public EmbeddedKafkaCluster(EmbeddedZookeeper zookeeper, List<Integer> ports) {
    this.zookeeper = zookeeper;
    this.zkConnection = zookeeper.getConnection();
    this.ports = resolvePorts(ports);
    this.baseProperties = new Properties();

    this.brokers = new ArrayList<KafkaServer>();
    this.logDirs = new ArrayList<File>();

    this.brokerList = constructBrokerList(this.ports);
  }
  public EmbeddedKafkaCluster(String zkConnection, Properties baseProperties, List<Integer> ports) {
    this.zkConnection = zkConnection;
    this.ports = resolvePorts(ports);
    this.baseProperties = baseProperties;

    this.brokers = new ArrayList<KafkaServer>();
    this.logDirs = new ArrayList<File>();

    this.brokerList = constructBrokerList(this.ports);
  }
  private List<Integer> resolvePorts(List<Integer> ports) {
    List<Integer> resolvedPorts = new ArrayList<Integer>();
    for (Integer port : ports) {
      resolvedPorts.add(resolvePort(port));
    }
    return resolvedPorts;
  }
  private int resolvePort(int port) {
    if (port == -1) {
      return TestUtils.getAvailablePort();
    }
    return port;
  }

  private String constructBrokerList(List<Integer> ports) {
    StringBuilder sb = new StringBuilder();
    for (Integer port : ports) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      sb.append("localhost:").append(port);
    }
    return sb.toString();
  }

  @Override public boolean isAutoStartup() {
    return true;
  }

  @Override public void stop(Runnable runnable) {

  }

  @Override public void start() {
    for (int i = 0; i < ports.size(); i++) {
      Integer port = ports.get(i);
      File logDir = TestUtils.constructTempDir("kafka-local");

      Properties properties = new Properties();
      properties.putAll(baseProperties);
      properties.setProperty("zookeeper.connect", zkConnection);
      properties.setProperty("broker.id", String.valueOf(i + 1));
      properties.setProperty("host.name", "localhost");
      properties.setProperty("port", Integer.toString(port));
      properties.setProperty("log.dir", logDir.getAbsolutePath());
      properties.setProperty("log.flush.interval.messages", String.valueOf(1));

      KafkaServer broker = startBroker(properties);

      brokers.add(broker);
      logDirs.add(logDir);
      isRunning=true;
      System.out.println("Started Brokers successfully");


    }
    Properties prop = new Properties();
    InputStream input = null;

//    try {
//      System.out.println("Creating topics");
//      input=EmbeddedKafkaCluster.class.getClassLoader().getResourceAsStream("topic.properties");
//
//      // load a properties file
//      prop.load(input);
//      ZkClient zkClient = new ZkClient(zookeeper.getConnection());
//      for(String key : prop.stringPropertyNames()) {
//        String value = prop.getProperty(key);
//       // AdminUtils.createTopic(zkClient, key, Integer.parseInt(value),1, new Properties());
//        System.out.println("Topic created "+key);
//      }
//
//    } catch (Exception ex) {
//      ex.printStackTrace();
//    } finally {
//      if (input != null) {
//        try {
//          input.close();
//        } catch (IOException e) {
//          e.printStackTrace();
//        }
//      }
//    }
  }

  private KafkaServer startBroker(Properties props) {
    KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime());
    server.startup();
    return server;
  }

  public Properties getProps() {
    Properties props = new Properties();
    props.putAll(baseProperties);
    props.put("metadata.broker.list", brokerList);
    props.put("zookeeper.connect", zkConnection);
    return props;
  }

  public String getBrokerList() {
    return brokerList;
  }

  public List<Integer> getPorts() {
    return ports;
  }

  public String getZkConnection() {
    return zkConnection;
  }

  @Override public void stop() {
    for (KafkaServer broker : brokers) {
      try {
        broker.shutdown();
        isRunning=false;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    for (File logDir : logDirs) {
      try {
        TestUtils.deleteFile(logDir);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
    }
  }

  @Override public boolean isRunning() {
    return isRunning;
  }

  @Override public int getPhase() {
    return 0;
  }
}
