package if4031.client.util;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * Created by nim_13512065 on 10/12/15.
 */
public class Topic {
    private static int sessionTimeoutMs = 10000;
    private static int connectionTimeoutMs = 10000;
    private static final ZkClient zkClient = new ZkClient("localhost:2181",
                                                            sessionTimeoutMs,
                                                            connectionTimeoutMs, ZKStringSerializer$.MODULE$);

    public static boolean isTopicExist(String topic) {
        return AdminUtils.topicExists(zkClient, topic);
    }

    public static void createTopic(String topic) {
        createTopic(topic, 1, 1);
    }

    public static void createTopic(String topic, int partitionCount, int replicationFactor) {
        AdminUtils.createTopic(zkClient, topic, partitionCount, replicationFactor, new Properties());
    }
}
