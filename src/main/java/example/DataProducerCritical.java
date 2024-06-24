package example;

import com.riferrei.kafka.core.BucketPriorityConfig;
import com.riferrei.kafka.core.BucketPriorityPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.commons.cli.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Critical data producer
 */
public class DataProducerCritical {

    private static final String TOPIC = "orders";
    private static final int RECORDS_COUNT = 100000;

    public static void main(String[] args) throws Exception {

        // Define the options
        Options options = new Options();

        Option input = new Option("priority", "priority", true, "priority");
        input.setRequired(true);
        options.addOption(input);


        Option delay = new Option("delayms", "delayms", true, "delay in mas");
        delay.setRequired(true);
        options.addOption(delay);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        cmd = parser.parse(options, args);

        String priority = cmd.getOptionValue("priority");

        long delayInMs = Long.parseLong(cmd.getOptionValue("delayms"));


        Properties producerCfg = new Properties();
        producerCfg.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "redpanda-0.testzone.local:31092");
        producerCfg.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerCfg.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producerCfg.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                BucketPriorityPartitioner.class.getName());
        producerCfg.put(BucketPriorityConfig.TOPIC_CONFIG, TOPIC);
        producerCfg.put(BucketPriorityConfig.BUCKETS_CONFIG, "Platinum, Gold");
        producerCfg.put(BucketPriorityConfig.ALLOCATION_CONFIG, "90%, 10%");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerCfg);


        // Produce data
        AtomicInteger counter = new AtomicInteger(1);

        for (int i =1; i <= RECORDS_COUNT; i++) {

            int value = counter.incrementAndGet();
            final String recordKey = priority + "-" + value;

            System.out.println("Producing data with key: " + recordKey);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, recordKey, "Value");
            producer.send(record, (metadata, exception) -> {
                System.out.println(String.format(
                        "Key '%s' was sent to partition %d",
                        recordKey, metadata.partition()));
            });
            try {
                Thread.sleep(delayInMs);
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        } // for

        producer.flush();
        producer.close();
    }

}
