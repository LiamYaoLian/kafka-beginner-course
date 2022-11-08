package kb;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
  public static void main(String[] args) {
    log.info("Creating a producer");

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    // create a producer record
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("demo_java", "hello world");

    // send data - async
    producer.send(producerRecord);

    // flush - sync
    producer.flush(); // block on this line until data has been sent

    // flush and close the producer
    producer.close();
  }

}

// kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic demo_java --partitions 3 --replication-factor 1
// kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic demo_java
