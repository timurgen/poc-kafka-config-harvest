package no.sysco.simplekafkaproducerapp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
            
            String jsonizedConf = MAPPER.writeValueAsString(config.values());
            producer.send(new ProducerRecord<>(TOPIC_NAME, clientId, jsonizedConf));

        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException | JsonProcessingException ex) {
            Logger.getLogger(ModifiedKafkaProducer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        } 
    }
}
