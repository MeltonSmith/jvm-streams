package smith.melton.multithreadconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author Melton Smith
 * @since 02.06.2025
 */
public class ConsumerTask implements Callable<Long> {
    private final List<ConsumerRecord<Object, Object>> records;

    public ConsumerTask(List<ConsumerRecord<Object, Object>> records) {
        this.records = records;
    }

    @Override
    public Long call() {
        for (ConsumerRecord record : records) {
            record.offset();
            // do something with record
        }
        //TODO
        return 0L;
    }


}
