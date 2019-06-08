package kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerDemo {
    private static final String topic = "mytopic";
    private static final Integer thread = 2;

    public static void main(String[] args) {
        Properties prop = new Properties();
        //设置zk的地址
        prop.put("zookeeper.connect", "hadoop001:2181,hadoop002:2181,hadoop003:2181");
        //设置本消费者的唯一标识，以防止重复消费
        prop.put("group.id", "myid");
        //smallest重最开始消费,largest代表重消费者启动后产生的数据才消费
        prop.put("auto.offset.reset", "smallest");
        //创建配置和消费者
        ConsumerConfig config = new ConsumerConfig(prop);
        ConsumerConnector consummer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, thread);
        //创建消息流
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consummer.createMessageStreams(topicMap);
        //获取目标主题的数据
        List<KafkaStream<byte[], byte[]>> streams = messageStreams.get(topic);
        //因为数据是实时产出的，所以要创建线程，读取kafka中该主题的消息
        if (streams != null) {
            for (KafkaStream<byte[], byte[]> stream : streams) {
                new Thread(() -> {
                    for (MessageAndMetadata<byte[], byte[]> msgMeta : stream) {
                        String msg = new String(msgMeta.message());
                        System.out.println(msg);
                    }
                }).start();
            }
        }
    }
}
