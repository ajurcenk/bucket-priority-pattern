package example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BucketConsumer implements Runnable {

    private static final String TOPIC = "orders";


    private static final int CONSUMERS_COUNT = 1;

    @SuppressWarnings("unchecked")
    private final KafkaConsumer<String,String>[] consumers = (KafkaConsumer<String, String>[]) new KafkaConsumer[CONSUMERS_COUNT];
    private final Properties consumerProp;

    private final int podIndex;

    private boolean isRunning = true;

    private String  consumerBucket = "";

    public BucketConsumer(Properties props, int podIndex, String consumerBucket) {

        this.podIndex = podIndex;
        this.consumerProp = props;
        this.consumerBucket = consumerBucket;
    }

    @Override
    public void run() {

        log("Creating consumers");
        try {

            for (int i = 0; i < this.consumers.length; i++) {
                final Properties consProps = new Properties();
                consProps.putAll(this.consumerProp);
                final String consumerId = "consumer-pod"+ podIndex+"-"+i;
                consProps.put("client.id", consumerId);
                // Static group membership
                // consProps.put("group.instance.id", consumerId);
                this.consumers[i] = new KafkaConsumer<>(consProps);
                this.consumers[i].subscribe(Collections.singletonList(TOPIC));
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException("Error creation consumers");
        }


        KafkaConsumer consumer = null;

        try {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            while (this.isRunning) {

                long processingDelay = random.nextLong(0, 1);
                int consumerIndex = random.nextInt(0, CONSUMERS_COUNT);

                // Random consumer
                consumer = this.consumers[consumerIndex];

                @SuppressWarnings("unchecked")
                // Issues with group rebalansing
                //ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(5));

                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log(String.format("[%s] Key = %s, Partition = %d",
                            Thread.currentThread().getName(), record.key(), record.partition()));
                }

            }
        } catch (WakeupException e) {
            // Handle shutdown
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        finally {
            if (consumer != null) {
                log("Closing...");
                consumer.close();
            }
        }
    }

    public void shutdown() {

        log("Stopping...");
        this.isRunning = false;
    }

    private void log(String msg) {

        System.out.println("pod:" + this.podIndex + " " + this.consumerBucket + " "  + msg);
        return;
    }
}
