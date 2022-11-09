package kb;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
  private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
  public static void main(String[] args) {
    log.info("Creating a consumer");
    String topic1 = "demo_java";

    // create properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "first_app");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    // create consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

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
  }

}

// Found no committed offset for partition demo_java-0
// Resetting offset for partition demo_java-0 to position FetchPosition{offset=0, offsetEpoch=Optional.empty, currentLeader=LeaderAndEpoch{leader=Optional[williams-air:9092 (id: 0 rack: null)], epoch=0}}.
// Poll from the earliest
// Stop
// Run again
// Setting offset for partition demo_java-0 to the committed offset FetchPosition{offset=14, offsetEpoch=Optional[0], currentLeader=LeaderAndEpoch{leader=Optional[williams-air:9092 (id: 0 rack: null)], epoch=0}}

