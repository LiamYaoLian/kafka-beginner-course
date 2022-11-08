package kb;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoShutdown {
  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoShutdown.class.getSimpleName());
  public static void main(String[] args) {
    log.info("Creating a consumer");
    String topic1 = "demo_java";

    // create properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "second_app");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

    // get a reference of the current thread
    final Thread mainThread = Thread.currentThread();

    // add the shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        log.info("Detected a shutdown. Exit by calling consumer.wakeup().");
        // next time calling consumer.poll(), will throw a wakeup exception,
        // then leave the while lopp
        consumer.wakeup();

        // join the main thread to allow the execution of the code in the main thread
        try {
          mainThread.join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

      }
    });

    try {
      // subscribe
      consumer.subscribe(Arrays.asList(topic1));

      while (true) {
        log.info("Polling");

        ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          log.info("Key: " + record.key() + ", Value:" + record.value());
          log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
        }
      }
    } catch (WakeupException e) {
      log.info("Wake up.");

      // ignore this exception because it is expected
    } catch (Exception e) {
      log.error("Unexpected exception", e);
    } finally {
      consumer.close(); // will commit the offset
      log.info("Consumer closed.");
    }



  }

}

