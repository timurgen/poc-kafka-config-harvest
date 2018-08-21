package no.sysco.simplekafkaproducerapp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Map;
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

    private static final String CONFIG_FIELD_NAME = "producerConfig";
    private static final String TOPIC_NAME = "__clients";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] FILTER = new String[]{
        "ssl.key.password", "ssl.keystore.password"
    };

    /**
     * Gather producer configuration and sends it to kafka topic
     *
     * @param producer KafkaProducer
     */
    public static final void harvest(final Producer producer) {

        try {
            Field configField = producer.getClass().getDeclaredField(CONFIG_FIELD_NAME);

            AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                configField.setAccessible(true);
                return null;
            });
            ProducerConfig config = (ProducerConfig) configField.get(producer);
            AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                configField.setAccessible(false);
                return null;
            });

            //FIXME need strategy for defining klient id if not set
            String clientId = "GENERATED_CLIENT_ID";

            Map<String, Object> configValues = config.valuesWithPrefixOverride("");
            Map<String, Object> filteredData = KafkaConfigHarverster.filterData(configValues, FILTER);

            String jsonizedConf = MAPPER.writeValueAsString(filteredData);
            producer.send(new ProducerRecord<>(TOPIC_NAME, clientId, jsonizedConf));

        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException | JsonProcessingException ex) {
            Logger.getLogger(ModifiedKafkaProducer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Masks config values. Only {@code String} values will be masked.
     *
     * @param configMap map with configuration to be filtered
     * @param filter keys to be maskes
     * @return filtered map
     */
    private static Map<String, Object> filterData(final Map<String, Object> configMap, final String[] filter) {
        Arrays.asList(filter).forEach((item) -> {
            //producer config map may contain particulary everything, we will mask only string values
            if (configMap.containsKey(item)) {
                configMap.replace(item, "XXXX-XXXX-XXXX-XXXX");
            }
        });
        return configMap;
    }

}
