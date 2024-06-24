package example;

import com.riferrei.kafka.core.BucketPriorityAssignor;
import com.riferrei.kafka.core.BucketPriorityConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ConsumerApp {

    private static final String TOPIC = "orders";
    private static final int INITIAL_POD_COUNT = 7;

    private ExecutorService executor;

    private final List<BucketConsumer> consumers;

    public ConsumerApp() {
        this.executor = Executors.newFixedThreadPool(20);
        this.consumers = new ArrayList<>();
    }

    public void start() throws Exception {

        boolean increasePods = true;
        int delayBeforeIncresedPodCountInMinutes = 1;
        int delayBeforeShutdownInMinutes = 3;
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        System.out.println("Create the initial pods set");
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "redpanda-0.testzone.local:31092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "bucket-consumer-v1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                BucketPriorityAssignor.class.getName());
        properties.put(BucketPriorityConfig.TOPIC_CONFIG, TOPIC);
        properties.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        properties.put(BucketPriorityConfig.ALLOCATION_CONFIG, "90%, 10%");


        int podIndex = 0;

        int numOfPlatinumConsumers = 9;
        int numOfColdConsumers = 1;

        properties.put(BucketPriorityConfig.BUCKET_CONFIG, "Platinum");

        for (int i = 1; i <= numOfPlatinumConsumers; i++) {

            final BucketConsumer consumerRunnable = new BucketConsumer(properties, podIndex, "Platinum");
            consumers.add(consumerRunnable);
            executor.submit(consumerRunnable);
            long podCreationDelay = random.nextLong(10, 20);
            try {
                Thread.sleep(podCreationDelay );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            podIndex++;
        }

        properties.put(BucketPriorityConfig.BUCKET_CONFIG, "Gold");
        for (int i = 1; i <= numOfColdConsumers; i++) {

            final BucketConsumer consumerRunnable = new BucketConsumer(properties, podIndex, "Gold");
            consumers.add(consumerRunnable);
            executor.submit(consumerRunnable);
            long podCreationDelay = random.nextLong(10, 20);
            try {
                Thread.sleep(podCreationDelay );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            podIndex++;
        }

        System.out.println("Waiting for shutdown");

        try {
            Thread.sleep(delayBeforeShutdownInMinutes * 60 * 1000L);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        System.out.println("Shutdown pods");
        this.shutdown();
    }

    public void shutdown() {
        for (BucketConsumer consumerRunnable : consumers) {
            consumerRunnable.shutdown();
        }
        // Stop executor
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                System.out.println("Timeout waiting for consumer threads to shut down");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        final ConsumerApp test = new ConsumerApp();
        test.start();
    }
}
