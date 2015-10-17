package if4031.client;

import if4031.client.util.RandomString;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class IRCClient {

    private final Object messageQueueLock = new Object();
    private final ProducerConfig producerConfig;

    private String nickname;
    private Set<String> joinedChannel = new HashSet<String>();

    public IRCClient(String server, int port) {
        Properties props = new Properties();
        props.put("metadata.broker.list", server + ":" + port);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // props.put("request.required.acks", "1"); kenapa ini di set 1? gapapa kok kalau message ga durable.

        producerConfig = new ProducerConfig(props);
        nickname = new RandomString().randomString(16);
    }

    void start() {
    }

    void stop() {
    }

    /**
     * Change nickname.
     * Nickname is stored in client because rabbitmq server cannot store it.
     *
     * @param newNickname new nickname.
     */
    public void changeNickname(String newNickname) {
        nickname = newNickname;
    }

    /**
     * Get messages from our queue in rabbitMQ.
     * Notify listener for the new messages.
     * TODO
     */
    public List<Message> getMessages() {
        return null;
    }

    /**
     * Join a channel.
     * Maybe no binding is required because the way kafka works.
     * Instead Attempt to consume all joined channels in getMessages.
     *
     * @param channelName channel to join
     */
    public void joinChannel(String channelName) {
        this.joinedChannel.add(channelName);
    }

    /**
     * Leave a channel.
     * Maybe no binding is required because the way kafka works.
     * Instead Attempt to consume all joined channels in getMessages.
     *
     * @param channelName channel to leave.
     */
    public void leaveChannel(String channelName) {
        this.joinedChannel.remove(channelName);
    }

    /**
     * Send Message to all joined channels.
     * joined channel is maintained in client.
     *
     * @param message message
     */
    public void sendMessageAll(String message) {
        String sent = "(" + nickname + "): " + message;
        Producer<String, String> producer = new Producer<String, String>(producerConfig);
        for (String mychannel : joinedChannel) {
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(mychannel, sent);
            producer.send(data);
        }
        producer.close();
    }

    /**
     * Send Message to a specific joined channel.
     * must check first whether we have joined the channel or not.
     * joined channel is maintained in client.
     *
     * @param channelName channel name
     * @param message     message
     */
    public void sendMessageChannel(String channelName, String message) {
        if (joinedChannel.contains(channelName)) {
            String sent = "(" + nickname + "): " + message;
            Producer<String, String> producer = new Producer<String, String>(producerConfig);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(channelName, sent);
            producer.send(data);
            producer.close();
        }
    }
}
