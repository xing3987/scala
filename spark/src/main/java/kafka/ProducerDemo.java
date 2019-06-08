package kafka;

import kafka.producer.KeyedMessage;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //create properties
        Properties props = new Properties();
        props.put("metadata.broker.list", "hadoop001:9092,hadoop002:9092,hadoop003:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //set config
        ProducerConfig config = new ProducerConfig(props);
        //create producer
        Producer<String, String> producer = new Producer(config);

        String topic = "mytopic";
        //send some msg
        for (int i = 0; i <= 100; i++) {
            producer.send(new KeyedMessage<String, String>(topic, "msg:" + i));
        }
    }
}
