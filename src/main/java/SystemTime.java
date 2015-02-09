/**
 * Created by ramdurga on 11/20/14.
 */
import kafka.utils.Time;

class SystemTime implements Time {
  public long milliseconds() {
    return System.currentTimeMillis();
  }

  public long nanoseconds() {
    return System.nanoTime();
  }

  public void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      // Ignore
    }
  }
}
