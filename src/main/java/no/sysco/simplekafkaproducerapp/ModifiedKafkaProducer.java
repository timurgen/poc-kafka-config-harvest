package no.sysco.simplekafkaproducerapp;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 *
 * @author 100tsa
 * @param <K>
 * @param <V>
 */
public class ModifiedKafkaProducer<K, V> extends KafkaProducer<String, Object> {

    private static final String TOPIC_NAME = "__clients";
    private String clientId;

    public ModifiedKafkaProducer(Map<String, Object> configs) {
        super(configs);
    }

    public ModifiedKafkaProducer(Properties properties) {
        super(properties);
        if(null == properties.getProperty("client.id")){
            this.clientId = "GENERATED_CLIENT_ID";
        }else {
            this.clientId = properties.getProperty("client.id");
        }
        createTopic(TOPIC_NAME, 1, (short) 1, properties);
        crawlConfigAndSendToBrocker();
    }

    private void crawlConfigAndSendToBrocker() {
        try {
            Field configField = this.getClass().getSuperclass().getDeclaredField("producerConfig");
            AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                configField.setAccessible(true);
                return null;
            });
            ProducerConfig config = (ProducerConfig) configField.get(this);
            AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                configField.setAccessible(false);
                return null;
            });
            config.values().forEach((key, value) -> {
                //System.out.println(String.format("\tkey: %-50s \t\t value: %s", key, value));
                this.send(new ProducerRecord<>(TOPIC_NAME, this.clientId, key + ":" + value));
            });
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException ex) {
            Logger.getLogger(ModifiedKafkaProducer.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Create new topic if not exists
     *
     * @param name topic name
     * @param partitions
     * @param rf replication factor
     * @param prop properties
     */
    private void createTopic(final String name, final int partitions, final short rf, Properties prop) {
        try (final AdminClient adminClient = KafkaAdminClient.create(prop)) {
            try {
                final NewTopic newTopic = new NewTopic(name, partitions, rf);
                final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
                createTopicsResult.values().get(name).get();
            } catch (InterruptedException | ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                // TopicExistsException - Swallow this exception, just means the topic already exists.
            }
        }
    }

}
