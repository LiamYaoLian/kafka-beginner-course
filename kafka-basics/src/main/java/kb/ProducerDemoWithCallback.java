package kb;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
  public static void main(String[] args) {
    log.info("Creating a producer");

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);



    for (int i = 0; i < 10; i++) {
      // create a producer record
      ProducerRecord<String, String> producerRecord =
          new ProducerRecord<>("demo_java", "msg " + i);

      // send data - async
      producer.send(producerRecord, new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
          // executed when msg is successfully sent or exception is thrown
          if (e == null) {
            // msg has been sent
            log.info("Received metadata\n" +
                "Topic: " + metadata.topic() + "\n" +
                "Partition: " + metadata.partition() + "\n" +
                "Offset: " + metadata.offset() + "\n" +
                "Timestamp: " + metadata.timestamp() + "\n");
          } else {
            log.error("Error while producing", e);
          }
        }
      });
    }



    // flush - sync
    producer.flush(); // block on this line until data has been sent

    // flush and close the producer
    producer.close();
  }

}

// kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic demo_java --partitions 3 --replication-factor 1
// kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic demo_java
