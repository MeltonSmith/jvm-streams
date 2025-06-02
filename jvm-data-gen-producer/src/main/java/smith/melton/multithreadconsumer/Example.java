package smith.melton.multithreadconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Melton Smith
 * @since 02.06.2025
 */
public class Example {

    private final KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(Map.of());

    private final ExecutorService backgroundProcessPool = Executors.newFixedThreadPool(8);

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private final Map<TopicPartition, Future<Long>> activeTasks = new HashMap<>();

    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

    private Long lastCommitedMills = 0l;

    public void runLoop() {
        try {
            consumer.subscribe(Collections.singleton("some-topic"));
            while (!stopped.get()) {
                ConsumerRecords<Object, Object> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                handleFetchedRecords(records);
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException wu) {
            if (!stopped.get()) {
                throw wu;
            }
        } finally {
            Utils.closeQuietly(consumer, "consumer");
        }
    }

    private void commitOffsets() {
        long interval = 500L;
        try {
            long l = System.currentTimeMillis();
            if (l - lastCommitedMills > interval) {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitedMills = l;
            }
        } catch (Exception wu) {
            System.out.println("Failed to commit offsets");
        }
    }

    private void handleFetchedRecords(ConsumerRecords<Object, Object> records) {
        if (records.count() > 0) {
            records.partitions().forEach(tp -> {
                List<ConsumerRecord<Object, Object>> records1 = records.records(tp);
                ConsumerTask task = new ConsumerTask(records1);
                Future<Long> submit = this.backgroundProcessPool.submit(task);
                activeTasks.put(tp, submit);
            });
            //resume when future is ready
            consumer.pause(records.partitions());
        }
    }

    private void checkActiveTasks() {
        ArrayList<TopicPartition> tpWithFinishedTasks = new ArrayList<>();
        activeTasks.forEach((tp, consumerTask) -> {
            if (consumerTask.isDone()) {
                tpWithFinishedTasks.add(tp);
                long offset = consumerTask.resultNow();
                if (offset > 0)
                    offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        });
        tpWithFinishedTasks.forEach(activeTasks::remove);
        consumer.resume(tpWithFinishedTasks);
    }

}
