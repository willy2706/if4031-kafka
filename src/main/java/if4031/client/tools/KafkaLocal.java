package if4031.client.tools;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * TODO mark for deletion
 */
public class KafkaLocal {
    private KafkaServerStartable kafkaServerStartable;
    private Properties prop;
    public KafkaLocal() throws IOException {
        prop = new Properties();
        InputStream inputStream = getClass().getResourceAsStream("/server.properties");

        if (inputStream != null) {
            prop.load(inputStream);
        } else {
            throw new FileNotFoundException("property file zookeeper.properties not found in the classpath");
        }
    }

    public void start () throws IOException {
        KafkaConfig kafkaConfig = new KafkaConfig(prop);
        new ZookeeperStarter().run();
        kafkaServerStartable = new KafkaServerStartable(kafkaConfig);
        kafkaServerStartable.startup();
    }

    public void stop() {
        kafkaServerStartable.shutdown();
    }

    public static void main(String[] args) throws IOException {
        KafkaLocal kafkaLocal = new KafkaLocal();
        try {
            kafkaLocal.start();
        } catch (IOException ex) {
            kafkaLocal.stop();
        }
    }

    public class ZookeeperStarter {
        private ZooKeeperServerMain zooKeeperServer;
        public ZookeeperStarter() {

        }
        public void run() throws IOException {
            Properties prop = new Properties();
            InputStream inputStream = getClass().getResourceAsStream("/zookeeper.properties");

            if (inputStream != null) {
                prop.load(inputStream);

            } else {
                throw new FileNotFoundException("property file zookeeper.properties not found in the classpath");
            }

            QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
            try {
                quorumConfiguration.parseProperties(prop);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }

            zooKeeperServer = new ZooKeeperServerMain();
            final ServerConfig configuration = new ServerConfig();
            configuration.readFrom(quorumConfiguration);

            new Thread() {
                public void run() {
                    try {
                        zooKeeperServer.runFromConfig(configuration);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }
    }

}
