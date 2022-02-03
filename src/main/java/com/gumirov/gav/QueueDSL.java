package com.gumirov.gav;

import com.gumirov.gav.queueDsl.EventObject;
import com.gumirov.gav.queueDsl.EventObjectFactory;
import com.gumirov.gav.queueDsl.EventQueue;
import com.gumirov.gav.queueDsl.QueueException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.d0sl.domain.DomainFunction;
import org.d0sl.domain.DomainModel;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.d0sl.machine.D0SL;
import org.d0sl.machine.SemanticException;
import org.everit.json.schema.ValidationException;
import org.json.JSONException;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

@DomainModel(name = "QueueDSL")
public class QueueDSL {
    private static final Logger log = LogManager.getLogger(QueueDSL.class);
    private static final String topic = "events";
    private Properties kafkaProps;
    private EventObjectFactory objectFactory = new EventObjectFactory();

    public QueueDSL() {}

    @DomainFunction(name = "set kafka props")
    public boolean setKafka(String brokers, String username, String password) {
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);

        String serializer = StringSerializer.class.getName();
        String deserializer = StringDeserializer.class.getName();

        kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", brokers);
        kafkaProps.put("group.id", "model.consumer");
        kafkaProps.put("enable.auto.commit", "true");
        kafkaProps.put("auto.commit.interval.ms", "1000");
        kafkaProps.put("auto.offset.reset", "earliest");
        kafkaProps.put("session.timeout.ms", "30000");
        kafkaProps.put("key.deserializer", deserializer);
        kafkaProps.put("value.deserializer", deserializer);
        kafkaProps.put("key.serializer", serializer);
        kafkaProps.put("value.serializer", serializer);
//        kafkaProps.put("security.protocol", "SASL_SSL");
//        kafkaProps.put("sasl.mechanism", "SCRAM-SHA-256");
//        kafkaProps.put("sasl.jaas.config", jaasCfg);
        return true;
    }

    @DomainFunction(name = "set val props")
    public boolean setValidationProps(String owl2jsonURI, String baseObjectURI) throws QueueException, IOException {
        this.objectFactory = new EventObjectFactory(owl2jsonURI, baseObjectURI);
        return true;
    }

    @DomainFunction(name = "get class uri")
    public String getClassUri(EventObject obj) {
        return obj.getUri();
    }

    @DomainFunction(name = "process queue")
    public boolean initQueue(String modelName, String processPredicateName) {
        kafkaProps.setProperty("group.id", modelName+".consumer"); // set group id

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(this.kafkaProps);
        consumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                EventObject obj = null;
                log.info(String.format("%s [%d] offset=%d, key=%s, value=\"%s\"",
                        record.topic(), record.partition(),
                        record.offset(), record.key(), record.value()));

                try {
                    obj = this.objectFactory.makeEventObject(record.value());
                } catch (JSONException e){
                    log.error("Event value is not a valid JSON!", e);
                } catch (ValidationException vex) {
                    log.error("Object is not valid!", vex);
                    log.error("Nested exceptions: " + vex.getAllMessages());
                } catch (IOException e) {
                    log.error("Can't validate object due to IO error!", e);
                } catch (QueueException e) {
                    log.error("Can't validate object due to internal exception or invalid class IRI!", e);
                }

                if (obj != null){
                    try {
                        D0SL.environment().callPredicate(modelName, processPredicateName, obj);
                    } catch (SemanticException e) {
                        log.error("Can't process the event!", e);
                    }
                }
            }
        }
    }

    // Event object methods:
    @DomainFunction(name = "get string parameter")
    public String getStrKey(EventObject obj, String key){
        try {
            return obj.getString(key);
        }catch (JSONException e){
            log.error("No such key in object: " + key);
            return "";
        }
    }

    @DomainFunction(name = "get numeric parameter")
    public Double getLongKey(EventObject obj, String key){
        try {
            return obj.getDouble(key);
        }catch (JSONException e){
            log.error("No such key in object: " + key);
            return 0.0;
        }
    }
}
