import kafka.producer.Partitioner;

/**
 * Created by nim_13512065 on 10/1/15.
 */
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner() {

    }
    @Override
    public int partition(Object o, int i) {
        int partition = 0;
        String stringKey = (String) o;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
            partition = Integer.parseInt( stringKey.substring(offset+1)) % i;
        }
        return partition;
    }
}
