import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * Created by nim_13512065 on 10/1/15.
 */
public class TestProducer {
    Logger logger = Logger.getLogger(TestProducer.class);
    private ProducerConfig config;
    public TestProducer() {
        Properties props = new Properties();

        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "SimplePartitioner");
        props.put("request.required.acks", "1");
        props.put("num.partitions","8");

        config = new ProducerConfig(props);
    }

    public String getIp() {
        Random rnd = new Random();
        String ip = "192.168.2." + rnd.nextInt(255);
        return ip;
    }

    public String getMessage(String ip) {
        long runtime = new Date().getTime();
        String msg = runtime + ",www.example.com," + ip;
        return msg;
    }

    public void run() {

        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        ZkClient zkClient = new ZkClient("localhost:2181",
                sessionTimeoutMs,
                connectionTimeoutMs, ZKStringSerializer$.MODULE$);

        String topicName = "woiwoi";
        int numPartitions = 8;
        int replicationFactor = 2;
        Properties topicConfig = new Properties();
        if (!AdminUtils.topicExists(zkClient, topicName)) {
            AdminUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, topicConfig); 
        }

        Producer<String, String> producer = new Producer<String, String>(config);
        String ip = getIp();
        String msg = getMessage(ip);

        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, ip, msg); //ga perlu key kan?
        producer.send(data);
        producer.close();
    }
}
