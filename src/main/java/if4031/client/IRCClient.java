package if4031.client;

import if4031.client.util.RandomString;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.List;
import java.util.Properties;

public class IRCClient {

    private final ChannelManager channelManager;
    private final Producer<String, String> producer;

    private String nickname;

    public IRCClient(String brokerAddress, String zookeeperAddress) {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerAddress);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<>(producerConfig);

        RandomString randomString = new RandomString();
        nickname = randomString.randomString(16);

        channelManager = new ChannelManager(zookeeperAddress, nickname);
    }

    public void start() {
    }

    public void stop() throws InterruptedException {
        producer.close();
        channelManager.destroy();
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
     * Get messages from our messages queue.
     */
    public List<Message> getMessages() {
        return channelManager.getMessages();
    }

    /**
     * Join a channel.
     * Maybe no binding is required because the way kafka works.
     * Instead Attempt to consume all joined channels in getMessages.
     *
     * @param channelName channel to join
     */
    public void joinChannel(String channelName) {
        channelManager.join(channelName);
    }

    /**
     * Leave a channel.
     * Maybe no binding is required because the way kafka works.
     * Instead Attempt to consume all joined channels in getMessages.
     *
     * @param channelName channel to leave.
     */
    public void leaveChannel(String channelName) throws InterruptedException {
        channelManager.leave(channelName);
    }

    /**
     * Send Message to all joined channels.
     * joined channel is maintained in client.
     *
     * @param message message
     */
    public void sendMessageAll(String message) {
        String sent = getSendString(message);
        for (String mychannel : channelManager.getChannelNames()) {
            KeyedMessage<String, String> data = new KeyedMessage<>(mychannel, sent);
            producer.send(data);
        }
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
        if (channelManager.containsChannel(channelName)) {
            String sent = getSendString(message);
            KeyedMessage<String, String> data = new KeyedMessage<>(channelName, sent);
            producer.send(data);
        }
    }

    private String getSendString(String message) {
        return "(" + nickname + "): " + message;
    }
}
