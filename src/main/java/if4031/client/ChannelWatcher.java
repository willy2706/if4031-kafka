package if4031.client;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;

/**
 * Channel Watcher.
 * Watch changes in channle and add it to message Queue.
 */
public class ChannelWatcher implements Runnable {
    private final ConsumerConnector consumer;
    private final KafkaStream stream;
    private final Queue<Message> messageQueue;
    private final Map<String, Boolean> joinedChannels;
    private final String channel;

    public ChannelWatcher(ConsumerConfig consumerConfig, String a_channel,
                          Queue<Message> a_messageQueue, Map<String, Boolean> a_joinedChannels) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCount = new HashMap<>();
        topicCount.put(a_channel, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topicCount);
        stream = streamMap.get(a_channel).get(0);

        messageQueue = a_messageQueue;
        joinedChannels = a_joinedChannels;

        channel = a_channel;
    }

    public void shutdown() {
        consumer.shutdown();
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String raw = new String(it.next().message());
            if (joinedChannels.containsKey(channel) && joinedChannels.get(channel)) {
                messageQueue.add(new Message(channel, raw));
            }
        }
    }
}
