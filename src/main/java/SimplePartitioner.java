import kafka.producer.Partitioner;

/**
 * TODO mark for deletion
 */
public class SimplePartitioner implements Partitioner {
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
