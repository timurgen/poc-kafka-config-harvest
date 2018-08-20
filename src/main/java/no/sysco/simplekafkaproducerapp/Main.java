package no.sysco.simplekafkaproducerapp;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author 100tsa
 */
public class Main {

    public static void main(String[] args) throws InterruptedException, ExecutionException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, Object> producer = new ModifiedKafkaProducer<>(props)) {

            while (true) {
                Future<RecordMetadata> metadata = producer.send(new ProducerRecord<>("infosak-case-created-v1", "case-0000004222" + System.currentTimeMillis(), "{\n"
                        + "  \"status\": \"IN_PROGRESS\",\n"
                        + "  \"assignedTo\": \"admin\",\n"
                        + "  \"case_id\": \"case-0000004\",\n"
                        + "  \"company\": \"COMPANY\",\n"
                        + "  \"case_type\": \"WORK_ORDER\",\n"
                        + "  \"date_created\": \"2018-07-03T06:25:16.000Z\",\n"
                        + "  \"date_updated\": \"2018-07-03T08:35:00.000Z\",\n"
                        + "  \"case_detail\": {\n"
                        + "    \"source\": \"A\",\n"
                        + "    \"destination\": \"B\",\n"
                        + "    \"title\": \"Break the leg\",\n"
                        + "    \"description\": \"Man1\",\n"
                        + "    \"priority\": 7,\n"
                        + "    \"metadata\": [\n"
                        + "      {\n"
                        + "        \"key\": \"KIS_objectID\",\n"
                        + "        \"value\": \"abc-1-1\"\n"
                        + "      },\n"
                        + "      {\n"
                        + "        \"key\": \"KIS_customerID\",\n"
                        + "        \"value\": \"abc-2-1\"\n"
                        + "      }\n"
                        + "    ],\n"
                        + "    \"message_id\": \"4\"\n"
                        + "  },\n"
                        + "  \"sub_cases\": 0\n"
                        + "}"));
                Thread.sleep(1000);
                System.out.println("Sent offset " + metadata.get().offset());
            }
        }
    }

}
