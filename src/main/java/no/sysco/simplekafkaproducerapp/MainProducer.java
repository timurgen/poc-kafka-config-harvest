package no.sysco.simplekafkaproducerapp;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author 100tsa
 */
public class MainProducer {

    public static void main(String[] args) throws InterruptedException, ExecutionException, NoSuchFieldException, IllegalAccessException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //we use interceptor to gather producer config and send them to the same kafka broker
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "no.sysco.simplekafkaproducerapp.ConfigHarvesterInterceptor");

        try (Producer<String, Object> producer = new KafkaProducer<>(props)) {
            while (true) {
                Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>("test-ropic", "case-0000004222" + System.currentTimeMillis(), "Foo"));
                Thread.sleep(1000);
                System.out.println("Sent offset " + metadata.get().offset());
            }
        }
    }

}
