/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package no.sysco.simplekafkaproducerapp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;

/**
 *
 * @author 100tsa
 * @param <K>
 * @param <V>
 */
public class ConfigHarvesterInterceptor<K, V> implements ProducerInterceptor<K, V>, ConsumerInterceptor<K, V> {

    private static final String TOPIC_NAME = "__clients";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            Properties props = new Properties();
            props.putAll(configs);

            //we need to remove interceptor or we are going to endless loop
            props.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
            props.remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
            props.remove(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
            props.remove(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);

            //we need to add own serializers which are not here if we intercept consumer or are not string if we are in 
            //KafkaStreams app
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Utils.createTopic(TOPIC_NAME, 1, (short) 1, props);
            Producer<String, Object> producer = Utils.getProducer(props);
            //we send config not props so we have all original values
            String jsonizedConf = MAPPER.writeValueAsString(configs);

            producer.send(new ProducerRecord<>(TOPIC_NAME, props.getProperty("client.id"), jsonizedConf));
        } catch (JsonProcessingException ex) {
            Logger.getLogger(ConfigHarvesterInterceptor.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex.getMessage(), ex);
        }

    }

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        //noop
    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        //noop
    }

    @Override
    public void close() {
        //noop
    }

}
