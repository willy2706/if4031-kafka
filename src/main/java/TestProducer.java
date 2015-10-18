import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * TODO mark for deletion
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
        Producer<String, String> producer = new Producer<String, String>(config);
        String ip = getIp();
        String msg = getMessage(ip);

        KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
        producer.send(data);
        producer.close();
    }
}
