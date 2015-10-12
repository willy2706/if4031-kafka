import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Belum bisa jalan yang ini
 */
public class SimpleExample {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put("zookeeper.connect", "localhost:2181");
        config.put("group.id", "default");
        config.put("rebalance.backoff.ms","5000");
        config.put("zookeeper.session.timeout.ms","10000");
        config.put("partition.assignment.strategy", "roundrobin");
        config.put("bootstrap.servers", "localhost:9092");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(config);

        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("halo", 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);

        List<KafkaStream<byte[], byte[]>> streamList = consumerMap.get("halo");

        KafkaStream<byte[], byte[]> stream = streamList.get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while(iterator.hasNext()) {
            System.out.println(new String(iterator.next().message()));
        }

    }
}