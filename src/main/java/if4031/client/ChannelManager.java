package if4031.client;

import kafka.consumer.ConsumerConfig;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Handles joining and leaving channel.
 */
public class ChannelManager {
    private final ConsumerConfig consumerConfig;

    private final Map<String, Boolean> joinedChannels = new ConcurrentHashMap<>();
    private final Map<String, ChannelWatcher> channelWatchers = new HashMap<>();
    private final Map<String, Thread> watcherThreads = new HashMap<>();
    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Construct Channel Manager
     * NOTE Channel Manager must be destroyed!
     *
     * @param zookeeperAddress zookeeper address, including port
     * @param groupID          consumer group ID
     */
    public ChannelManager(String zookeeperAddress, String groupID) {
        System.out.println("groupID: " + groupID);
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperAddress);
        props.put("group.id", groupID);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        consumerConfig = new ConsumerConfig(props);
    }

    /**
     * Destroy Channel Manager
     */
    public void destroy() throws InterruptedException {
        for (String channel : channelWatchers.keySet()) {
            ChannelWatcher watcher = channelWatchers.get(channel);
            watcher.shutdown();
        }

        for (String channel : watcherThreads.keySet()) {
            Thread thread = watcherThreads.get(channel);
            thread.join(); // throws InterupptedException
        }
    }

    /**
     * Join a channel.
     *
     * @param channel channel name
     */
    public void join(String channel) {
        if (!watcherThreads.containsKey(channel)) {
            ChannelWatcher channelWatcher = new ChannelWatcher(consumerConfig, channel, messageQueue, joinedChannels);
            channelWatchers.put(channel, channelWatcher);
            Thread watcherThread = new Thread(channelWatcher);
            watcherThread.start();
            watcherThreads.put(channel, watcherThread);
        }

        joinedChannels.put(channel, true);
    }

    /**
     * Leave a channel.
     * Basically the reverse of join a channel.
     *
     * @param channel channel name
     */
    public void leave(String channel) throws InterruptedException {
        joinedChannels.put(channel, false);
    }

    /**
     * Get joined channel names.
     *
     * @return joined channel names
     */
    public String[] getChannelNames() {
        return joinedChannels.keySet().toArray(new String[joinedChannels.size()]);
    }

    /**
     * Determine whether channel is in joined channels list.
     *
     * @param channel channel name
     * @return true if channel is in joined channels list.
     */
    public boolean containsChannel(String channel) {
        return joinedChannels.containsKey(channel);
    }

    /**
     * Get all accumulated messages.
     *
     * @return accumulated messages.
     */
    public List<Message> getMessages() {
        List<Message> result = new ArrayList<>();
        while (!messageQueue.isEmpty()) {
            result.add(messageQueue.remove());
        }
        return result;
    }
}
