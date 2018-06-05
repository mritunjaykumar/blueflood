package com.rackspacecloud.blueflood.inputs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.types.IMetric;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class MetricsProducer {
    Properties properties;
    Producer<String, String> producer;

    private static final String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String KAFKA_PRODUCER_ACKS = "acks";
    private static final String KAFKA_PRODUCER_RETRIES = "retries";
    private static final String KEY_SERIALIZER = "key.serializer";
    private static final String VALUE_SERIALIZER = "value.serializer";

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsProducer.class);

    public MetricsProducer(){
        properties = new Properties();
        properties.setProperty(KAFKA_BOOTSTRAP_SERVERS, CoreConfig.KAFKA_BOOTSTRAP_SERVERS.getDefaultValue());
        properties.setProperty(KAFKA_PRODUCER_ACKS, CoreConfig.KAFKA_PRODUCER_ACKS.getDefaultValue());
        properties.setProperty(KAFKA_PRODUCER_RETRIES, CoreConfig.KAFKA_PRODUCER_RETRIES.getDefaultValue());

        properties.setProperty(KEY_SERIALIZER, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER, StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);

        LOGGER.info("Created a new Kafka producer to kafka bootstrap servers {}",
                CoreConfig.KAFKA_BOOTSTRAP_SERVERS.getDefaultValue());
    }

    public void send(List<IMetric> metrics) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(CoreConfig.KAFKA_METRICS_TOPIC_NAME.getDefaultValue(),
                            objectMapper.writeValueAsString(metrics));

            producer.send(record, (recordMetadata, e) -> {
                if(e == null){
                    LOGGER.debug("Successfully sent record {} to Kafka...", record);
                    LOGGER.debug("Record metadata is [{}]", recordMetadata);
                }
                else {
                    LOGGER.error("Failed to send record {}", record);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Unable to write metrics collection into json. Exception message: [{}]", e.getMessage());
            // Log error, but continue with next item
        }
    }
}
