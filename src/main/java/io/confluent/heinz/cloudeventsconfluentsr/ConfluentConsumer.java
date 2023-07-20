/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.heinz.cloudeventsconfluentsr;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.heinz.avroMsgK;
import io.confluent.heinz.refSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfluentConsumer {
    private final Log logger = LogFactory.getLog(ConfluentConsumer.class);
    private KafkaProducer producer;
    private Environment env;
    private String topic = "";

    public ConfluentConsumer(Environment env) {
        logger.info("Check for brokers: " + env.getProperty("bootstrap.servers"));
        this.env = env;
        createConfluentSession(env);
    }

    public void createConfluentSession(Environment env) {
        AtomicBoolean running = new AtomicBoolean(true);

        ObjectMapper mapper = new ObjectMapper();

        Properties props = new Properties();
        this.topic = env.getProperty("topic");

        props.setProperty("bootstrap.servers", env.getProperty("bootstrap.servers"));
        props.setProperty("schema.registry.url", env.getProperty("schema.registry.url"));
        props.setProperty("schema.registry.basic.auth.user.info",
                env.getProperty("schema.registry.basic.auth.user.info"));
        props.setProperty("basic.auth.credentials.source", env.getProperty("basic.auth.credentials.source"));
        props.setProperty("sasl.mechanism", env.getProperty("sasl.mechanism"));
        props.setProperty("sasl.jaas.config", env.getProperty("sasl.jaas.config"));
        props.setProperty("security.protocol", env.getProperty("security.protocol"));
        props.setProperty("client.dns.lookup", env.getProperty("client.dns.lookup"));
        props.setProperty("acks", "all");
        props.setProperty("auto.create.topics.enable", "false");
        props.setProperty("topic.creation.default.partitions", "3");
        props.setProperty("auto.register.schema", "false");
        props.setProperty("json.fail.invalid.schema", "true");
        props.setProperty("enable.idempotence", env.getProperty("enable.idempotence"));
        props.setProperty("key.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer.class.getName());
        props.setProperty("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer",
                io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer.class.getName());
        props.put(KafkaJsonSchemaDeserializerConfig.USE_LATEST_VERSION, "true");
        props.put(KafkaJsonSchemaDeserializerConfig.AUTO_REGISTER_SCHEMAS, "false");
        props.put(KafkaJsonSchemaDeserializerConfig.FAIL_INVALID_SCHEMA, "true");
        props.put(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY, "javaType");
        //props.put("latest.compatibility.strict", "false");
        props.put("latest.compatibility.strict", "true");
        props.setProperty("group.id", env.getProperty("consume.group.id"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Consumer");
            running.set(false);
        }));

        try (final Consumer<avroMsgK, refSchema> consumer = new KafkaConsumer<avroMsgK, refSchema>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            logger.info("subscriber subscription! set");

            while (running.get()) {
                ConsumerRecords<avroMsgK, refSchema> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<avroMsgK, refSchema> record : records) {

                    avroMsgK aKey = record.key();
                    String key = aKey.toString();

                    refSchema refValue = record.value();
                    String value = refValue.toString();
                    System.out.println("+++++++++++++++++++++++++++++++");
                    System.out.println(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));

                    System.out.println("data customerID field: " + refValue.getData().getCustomerId());
                    System.out.println("+++++++++++++++++++++++++++++++ \n");
                }
            }
        }
    }
}
