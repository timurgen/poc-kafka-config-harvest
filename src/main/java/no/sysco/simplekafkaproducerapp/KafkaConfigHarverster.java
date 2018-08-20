package no.sysco.simplekafkaproducerapp;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 *
 * @author 100tsa
 */
public class KafkaConfigHarverster {
    private static final String TOPIC_NAME = "__clients";

    public static final void harvest(Producer producer) {

        try {
            Field configField = producer.getClass().getDeclaredField("producerConfig");
            AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                configField.setAccessible(true);
                return null;
            });
            ProducerConfig config = (ProducerConfig) configField.get(producer);
            AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                configField.setAccessible(false);
                return null;
            });
            String clientId = "GENERATED_CLIENT_ID";
            
            config.values().forEach((key, value) -> {
                System.out.println(String.format("\tkey: %-50s \t\t value: %s", key, value));
                producer.send(new ProducerRecord<>(TOPIC_NAME, clientId, key + ":" + value));
            });
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(ModifiedKafkaProducer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }
}
