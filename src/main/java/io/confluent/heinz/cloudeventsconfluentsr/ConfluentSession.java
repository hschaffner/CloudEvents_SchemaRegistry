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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import io.confluent.heinz.*;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.*;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.heinz.avroMsgK;
import io.confluent.heinz.JsonMsg;
import org.springframework.stereotype.Service;

@Component
public class ConfluentSession{
    private final Log logger = LogFactory.getLog(ConfluentSession.class);
    private KafkaProducer<avroMsgK, ObjectNode> producerCfltCe;
    private String topic = "";
    private int counter = 0;
    private Properties props;
    private JsonSchema jsonSchema1 = null;
    private Environment env = null;

    private Map<String, JsonNode> resolvedRefsNodes;
    private List<SchemaReference> sReferences;

    //Constructor
    public ConfluentSession(Environment env) {
        createConfluentSession(env);
        createConfluentProducer();
    }

        //ConfluentConsumer cons = new ConfluentConsumer(env);
    //}

    public void createConfluentSession(Environment env) {
        this.env = env;
        props = new Properties();

        topic = env.getProperty("topic");

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

    }

    private void createConfluentProducer() {
        AtomicBoolean running = new AtomicBoolean(true);
        if (producerCfltCe == null) {
            logger.info("Creating new Structured CE Producer");

            //additional Confluent session properties related to Schema Registry for Complex schemas
            props.setProperty("key.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());
            props.setProperty("value.serializer",
                    io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer.class.getName());
            props.put(KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, "true");
            props.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, "false");
            props.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, "true");
            props.put(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY, "javaType");
            //props.put("latest.compatibility.strict", "false");
            props.put("latest.compatibility.strict", "true");
            props.put("client.id", env.getProperty("producer.id"));

            //Using customer partitioner to test partitioning using the AvroKey Customer ID
            //Also available from returned metadata from the send()
            //props.put("partitioner.class", JSONValueAvroKeyPartitioner.class);
            //Below is a second partitioner that does not extend the default partitioner
            props.put("partitioner.class", CustomAvroJsonPartitioner.class);

            //Create the Confluent producer
            producerCfltCe = new KafkaProducer<>(props);

            //Create the schema that understands schema references in the Schema Registry
            jsonSchema1 = getSchemaWithRefs(env.getProperty("topic"));

            logger.info("-=-=-=-=-=-=-=-=-=-=-=-=-=-=- created producer");

            //shutdown hook when process is interrupted
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Stopping Producer");
                producerCfltCe.flush();
                producerCfltCe.close();
                running.set(false);
            }));

        }
    }


    //Called from Rest Controller Listener
    public void sendJsonMessageCE(JsonMsg jMsg) {
        System.out.println("------------------------------------------ ");
        String CE_JsonEvent;


        //increasing the Customer ID in the Key to forcing writing to more than one partition
        // but maintaining order per ID
        counter++;
        if (counter == 9) {
            counter = 0;
        }

        //Get String request data from Rest listener to display in logs
        String JsonStr = "";
        ObjectMapper mapper = new ObjectMapper();
        //Output the POST message to confirm what was received
        try {
            JsonStr = mapper.writeValueAsString(jMsg);
        } catch (JsonProcessingException je) {
            logger.info("++++++++++++++++++++JSON Error: \n:");
            je.printStackTrace();
        }
        logger.info("REST request data in sender method: " + JsonStr);

        //generate Record Key from mvn generated POJO from schema
        avroMsgK msgK = new avroMsgK();
        msgK.setClient("heinz57"); //Hard Coded for simplicity of sample
        msgK.setClientID(jMsg.getCustomerId() + counter);

        //Used for CloudEvents ID
        String id = UUID.randomUUID().toString();

        //POJO for value schema generated from imported Value Schema for topic imported from Schema Registry
        refSchema refSchemaCE = new refSchema();

        //POJO for the "data" property that is generated by maven for use as the schema reference for the topic value
        JsonMsg dataCE = new JsonMsg();
        dataCE.setCustomerId(jMsg.getCustomerId() + counter);
        dataCE.setFirstName(jMsg.getFirstName());
        dataCE.setLastName(jMsg.getLastName());

        //Produce the rest of the CloudEvents JSON message
        refSchemaCE.setData(dataCE);
        refSchemaCE.setId(id);
        refSchemaCE.setType("confluent CE Sample");
        refSchemaCE.setSource("https://github.com/cloudevents/sdk-java/tree/main/examples/kafka");
        refSchemaCE.setSpecversion("1.0");

        // Produce the event
        mapper = new ObjectMapper();
        JsonNode actualObj = null;
        //Create the object that will be published as a JSON Node
        try {
            CE_JsonEvent = mapper.writeValueAsString(refSchemaCE);
            actualObj = mapper.readTree(CE_JsonEvent);
            logger.debug("Actual JsonNode: " + actualObj.toString());
        } catch (JsonProcessingException je) {
            System.out.println("JSON Error: \n:");
            je.printStackTrace();
        }


        //Send the Cloud Event message
        RecordMetadata metadata;
        try {
            metadata = producerCfltCe.send(new ProducerRecord<>(topic, msgK,
                            JsonSchemaUtils.envelope(jsonSchema1, actualObj)), new MyProducerCallback())
                          .get();
            /* The below code also works as an alternative to the code above
            metadata = producerCfltCe.send(new ProducerRecord<>(topic, msgK,
              JsonSchemaUtils.envelope(jsonSchema1.toJsonNode(), sReferences, resolvedRefsNodes, actualObj)), new MyProducerCallback())
                    .get();
             */
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        logger.info("Record sent to partition " + metadata.partition() + ", with offset " + metadata.offset());

        System.out.println("------------------------------------------ \n");

    }

    private String schemaFromFile (String property) {

        Path path;
        try {
            path = Paths.get(getClass()
                    .getResource(env.getProperty(property)).toURI());

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        Stream<String> lines;
        try {
            lines = Files.lines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String data = lines.collect(Collectors.joining("\n"));
        lines.close();
        return data;
    }

    //This method is used for complex schemas and Schema Registry
    private JsonSchema getSchemaWithRefs(String topicName) {

        SchemaMetadata meta;
        String schemaRegistryUrl = props.getProperty("schema.registry.url");
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10, (Map)props);
        try {
            meta = schemaRegistryClient.getLatestSchemaMetadata(topicName + "-value");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (RestClientException e) {
            throw new RuntimeException(e);
        }

        //Generate the Reference map that maps the reference name to a reference schema
        Map<String, String> mutableRefMap = new HashMap<>();
        Map<String, JsonNode> mutableRefMapNode = new HashMap<>();
        Iterator<SchemaReference> referenceIterator = meta.getReferences().iterator();
        while(referenceIterator.hasNext()) {
            SchemaReference sr = referenceIterator.next();
            logger.info("+++++++ Schema Reference name: " + sr.getName() + ", Reference version: " + sr.getVersion() + ", Reference Subject: " + sr.getSubject());
            try {
                SchemaMetadata metaRef = schemaRegistryClient.getSchemaMetadata(sr.getSubject(), sr.getVersion());
                String _refSchema = metaRef.getSchema();
                JsonSchema refSchema = new JsonSchema(_refSchema);
                mutableRefMap.put(sr.getName(), refSchema.canonicalString());
                mutableRefMapNode.put(sr.getName(), refSchema.toJsonNode());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
        }

        //Immutable map of reference name and associated schema
        ImmutableMap<String, String> resolvedRefs = ImmutableMap.<String, String>builder()
                .putAll(mutableRefMap)
                .build();

        ImmutableMap<String, JsonNode> resolvedRefsNodes = ImmutableMap.<String, JsonNode>builder()
                .putAll(mutableRefMapNode)
                .build();

        this.sReferences = meta.getReferences();
        this.resolvedRefsNodes = resolvedRefsNodes;
        //return a Confluent JsonSchema object that understands the mapping of SR reference names in the main schema
        return new JsonSchema(meta.getSchema(), meta.getReferences(), resolvedRefs, null);
    }

    class MyProducerCallback implements Callback {

        private final Log logger = LogFactory.getLog(MyProducerCallback.class);

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null)
                logger.info("AsynchronousProducer failed with an exception");
            else {
                logger.info("AsynchronousProducer call Success:" + "Sent to partition: " + recordMetadata.partition() + " and offset: " + recordMetadata.offset() + "\n");
            }

        }
    }
}
