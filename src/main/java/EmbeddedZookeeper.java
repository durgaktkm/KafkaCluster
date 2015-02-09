import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;

import static org.apache.zookeeper.server.NIOServerCnxnFactory.createFactory;

/**
 * Created by ramdurga on 2/8/15.
 */
public class EmbeddedZookeeper implements SmartLifecycle{
  private static final Logger logger = LoggerFactory.getLogger(EmbeddedZookeeper.class);
  private int port = -1;
  private int tickTime = 500;

  private ServerCnxnFactory factory;
  private File snapshotDir;
  private File logDir;
  private boolean isRunning=false;

  public EmbeddedZookeeper() {
    this(-1);
  }

  public EmbeddedZookeeper(int port) {
    this(port, 500);
  }

  public EmbeddedZookeeper(int port, int tickTime) {
    this.port = resolvePort(port);
    this.tickTime = tickTime;
  }

  private int resolvePort(int port) {
    if (port == -1) {
      return TestUtils.getAvailablePort();
    }
    return port;
  }
  @Override public boolean isAutoStartup() {
    return true;
  }

  @Override public void stop(Runnable runnable) {

  }

  @Override public void start() {

    if (this.port == -1) {
      this.port = TestUtils.getAvailablePort();
    }
    try {
    this.factory = createFactory(new InetSocketAddress("localhost", port), 1024);
    this.snapshotDir = TestUtils.constructTempDir("embeeded-zk/snapshot");
    this.logDir = TestUtils.constructTempDir("embeeded-zk/log");
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
      isRunning=true;
      System.out.println("Started Zookeeper with Port"+port);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void stop() {
    factory.shutdown();
    isRunning=false;
    try {
      TestUtils.deleteFile(snapshotDir);
    } catch (FileNotFoundException e) {
      // ignore
    }
    try {
      TestUtils.deleteFile(logDir);
    } catch (FileNotFoundException e) {
      // ignore
    }
  }

  @Override public boolean isRunning() {
    return isRunning;
  }

  @Override public int getPhase() {
    return 0;
  }
  public String getConnection() {
    return "localhost:" + port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public void setTickTime(int tickTime) {
    this.tickTime = tickTime;
  }

  public int getPort() {
    return port;
  }

  public int getTickTime() {
    return tickTime;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
    sb.append("connection=").append(getConnection());
    sb.append('}');
    return sb.toString();
  }
}
