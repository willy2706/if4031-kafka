package if4031.client;

import if4031.client.util.RandomString;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class IRCClient {

    private final Object messageQueueLock = new Object();
    private final ProducerConfig producerConfig;

    private String nickname;
    private Set<String> joinedChannel = new HashSet<String>();

    public IRCClient(String server, int port) throws IOException, TimeoutException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        producerConfig = new ProducerConfig(props);
        nickname = new RandomString().randomString(16);
    }

    void start() {
    }

    void stop() throws IOException {
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
     * //TODO
     */
    public List<Message> getMessages() {
        return null;
    }

    /**
     * Join a channel.
     * Equivalent to binding to our queue to an exchange in rabbitgMQ.
     * kafka menyediakan fitur untuk langsung buat topic dengan default tertentu
     * @param channelName channel to join
     */
    public void joinChannel(String channelName) throws IOException {
        this.joinedChannel.add(channelName);
    }

    /**
     * Leave a channel.
     * Equivalent to unbinding our queue from an exchange in rabbitMQ.
     *
     * @param channelName channel to leave.
     */
    public void leaveChannel(String channelName) throws IOException {
        this.joinedChannel.remove(channelName);
    }

    /**
     * Send Message to all joined channels.
     * Equivalent to sending messages to many exchanges in rabbitMQ.
     * joined channel is maintained in client.
     *
     * @param message message
     */
    public void sendMessageAll(String message) throws IOException {
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
     * Equivalent to sending message to an exchange in rabbitMQ.
     * must check first whether we have joined the channel or not.
     * joined channel is maintained in client.
     *
     * @param channelName channel name
     * @param message     message
     */
    public void sendMessageChannel(String channelName, String message) throws IOException {
        if (joinedChannel.contains(channelName)) {
            String sent = "(" + nickname + "): " + message;
            Producer<String, String> producer = new Producer<String, String>(producerConfig);
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(channelName, sent);
            producer.send(data);
            producer.close();
        }
    }
}
