# JSON CloudEvents Sample using Confluent Schema Registry and JSON Reference Schema

## Introduction

CloudEvents is a CNCF Specification for describing events in a common way. 
CloudEvents also provides an API for multiple different protocol bindings, including Kafka. More details on CloudEvents can be found here:

https://cloudevents.io/

CloudEvents specifies two message types "Structured-Mode Message" and "Binary-Mode Message". Binary-mode places the CloudEvents
attributes into the Message header and the payload for the message is essentially considered a binary object. Structured-Mode
transports a JSON object (typically, but other formats are potentially supported) that is
used as the payload for the messaging object. The JSON message contains the CloudEvetns attributes and the actual JSON event data.

This sample is focused only on structured-mode CloudEvents. 

Within the JSON definition of the CloudEvents message, there are several mandatory and optional attributes deinfined for the JSON payload. 
One of the optional attributes is the "dataschema". This is expected to only be used during
development. As outlined in the CloudEvents specification:

_"The dataschema attribute is expected to be informational, largely to be used during development and by tooling that is able to provide diagnostic information over arbitrary CloudEvents with a data content type understood by that tooling._

_When a CloudEvent's data changes in a backwardly-compatible way, the value of the dataschema attribute should generally change to reflect that. An alternative approach is for the URI to stay the same, but for the content served from that URI to change to reflect the updated schema. The latter approach may be simpler for event producers to implement, but is less convenient for consumers who may wish to cache the schema content by URI."_

Part of Confluent's data management strategy includes their Schema Registry. Unlike the CloudEvents payload attribute, Confluent's Schema Registry supports the efficient transport of the schema
on a per-message basis. The schema reference is also part of the API serialization/deserialization of the payload messages and also supports
a different schema for the Confluent record value and record key. The registry also supports and recognize multiple schema migraiton strategies beyond simple CloudEvents backward compatibility. The Confluent brokers
can be configured to also participate in the schema strategy where published messages that do not adhere to the schemas in the registry associated with the topic will be rejected.

More details on Schema Registry can be found here:

https://docs.confluent.io/platform/current/schema-registry/index.html

Unfortunately, the CloudEvents SDK for structured-mode using the Kafka protocol binding does not support Confluent's Schema Registry. This project make use of the CloudEvents JSON 
schema, but is used in context of the Confluent Schema Registry and the Confluent JSON serialization/deserialization classes that support the Schema Registry. 

The most practical method to support CloudEvents is to use "complex" JSON schema that makes use of the "$ref" keyword in the schema to allow a recursive validatable structure to other schemas. In the Confluent case, the referenced schema is the actual data schema that is then essentially "wrapped" in the CloudEvents schema. Therefore, the CloudEvents schema will never really change other than the references to the JSON payload schema that is addressed in the CloudEvents schema as a reference.
The Confluent Schema Registry supports JSON references and complex schemas. 

## Schema Registry and JSON $ref Support

When using Confluent Schema Registry it is necessary to send the schema with each message. Since Topics can have multiple records with different versions of the same schema (or even different schemas), it is necessary to include the schema with each message to ensure proper deserialization by the consumer against the topic messages.
However, it is inefficient to send the complete schema with each message. Fortunately, Schema Registry will substitute and forward an index number for the specific schema and the actual schema is cached in the Confluent application. 

With Avro, the schema is automatically forwarded. With JSON, it is necessary to make use of several Confluent JSON utility classes to facilitate the transfer of the schema with the actual JSON payload.
These Confluent JSON utilities include: JsonSchema and JsonSchemaUtils. The processing of JSON messages and referenced schemas is also simplified using the Java Schema Registry client classes to get details
about the stored schemas.

### Common Error when using Complex Schemas with Confluent Schema Registry

While references are supported in a JSON schema that is stored in Confluent Schema Registry, it is necessary to prepare the producer to
send the complex schema or there will be protocol or serialization errors thrown if the regular schema is included in the producer's send method.

If the reference is included in the schema as:

```
"data": {
   "$ref": "JsonMsg.json",
   "description": "The event payload."
},
```

In this case the refernce schema is labeled with the name "JsonMsg.json". This name is configured to point to the stored reference's registry  subject name. If the variable jsonSchema1 is a JsonSchema class that was created from the actual schema string that includes the above reference. Then when you send the record with:

```
metadata = producerCfltCe.send(new ProducerRecord<>(topic, msgK,
            JsonSchemaUtils.envelope(jsonSchema1, actualObj)), new MyProducerCallback())
             .get();
```

you will get an exception since the "jsonSchema1" does not understand the reference and throws an error since it expects the $ref attribute to be a URI and obviously this is not a properly formed URI. As a result you get an error similar to:

```
Caused by: java.io.UncheckedIOException: java.net.MalformedURLException: no protocol: JsonMsg.json
```

If you are using multiple schemas in the same topic, the schema should include something similar to:

```
"data": {
      "oneOf": [
        {
          "$ref": "JsonMsg.json"
        }
      ],
      "description": "The event payload."
    }
```

In this case there is only single JSON references, however in practice you would include all the schema references for all the potential schemas  used for messages in the same topic. However, you will get a new error thrown: 

```
Caused by: java.io.IOException: Incompatible schema
```

There is a workaround for both errors, but it is not recommended. Basically, the documentation suggests setting:

```
properties.setProperty("latest.compatibility.strict","false");
```

I do not recommend this workaround. The message will successfully be sent to Confluent, but there is no validation of the schema reference.

The issue that is causing the exception is that Schema Registry uses its own internal mechanisms to point to schema references. This is all based on the JsonSchema class from Confluent. If you use the default and simply pass the CloudEvents schema with the $ref tag, it will assume this should be followed by a proper URL pointing to the schema reference. To make sure you are 
using the internal Confluent reference capabilities, you must use the JsonSchema utility with a method signature that has several more arguments. In this sample, a method is included that creates the required JsonSchema object that understands the Schema Registry JSON reference when creating a complex JSON Schema. Consider the following:

```
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
```

This method creates a cached Schema Registry client and then retrieves the metadata for the stored schema subject "topicname-value". Included in the metadata that is returned are the schema references that have been 
registered for the schema associated with the schema subject ( the topic name appended with "-value"). The method 
then iterates over all the associated references and gets the reference schema from the Schema Registry using the reference names and stores them in a Map of JsonSchemas. It also stores a map of the JsonSchema as JsonNodes. 

Finally, the method stores the maps as ImmutableMaps. Notice that the JsonSchema that is created no longer based on just the original string-based JSON schema (in this case the CloudEvents schema with a reference for the data attribute). The JsonSchema object is created to include a map of the schema reference labels and the map of the associated referencable schemas. When this JSON schema is used to publish the record
there will be no exceptions thrown since it understands how to iterate and validate the referenced schema(s) found in the CloudEvents schema. In this test sample there is only one reference, but this will work even if there are multiple referenced schemas in the CloudEvent schema.

A sample of the send method and the associated arguments has already been referenced above. However, there is an alternative send method when schemas with reference are used against Confluent schema registry. An alternative is:

```
metadata = producerCfltCe.send(new ProducerRecord<>(topic, msgK,
              JsonSchemaUtils.envelope(jsonSchema1.toJsonNode(), sReferences, resolvedRefsNodes, actualObj)), new MyProducerCallback())
                    .get();
```

With the above send method, the details for the schema and references are used directly. However, it is important to note, that the arguments are based on JsonNode class and a map of JsonNodes. That is why the method to create the referenced schema also created a map of the JsonNode of the JsonSchema. 

## Deploy and Set up Project

It is assumed the developer is already familiar with Maven, Spring, Confluent and Java. The repository is based on an Intellij Project.

This is a Spring project that can be downloaded or cloned. It is created with the expectation that the Confluent infrastructure is available in Confluent Cloud. A free cloud account can be created for testing. 

Once the Confluent Cloud is setup and created, it is expected the appropriate keys for Confluent Cloud and Confluent Schema Registry are updated in the "application.yaml" and "pom.xml" files. 

In this example a schema was stored that was called "CloudEvents_Reference". It is possible to create the schema that is used for the reference without creating a topic. From the Confluent Cloud Environment where your cluster is created it is possible to use the Stream Governence Package (found on the right of the page that lists the Cloud Environments) to create a schema directly. The schema used for the reference is found in the project under the "jsonschema" folder and is called "JsonMsg.json". Simply copy this schema into the Schmea Registry schema editor.

At this point it is possible to create a topic in your cluster. This sample was based on a topic call "CloudEvents". Once the Topic is created 
it is possible to add the JSON value schema and the Avro key schema for the topic. An Avro schema is generally recommended for record keys if a full schema is required for the record key. The Avro Schema to use if found under the "avroschema" folder. The record value schema for the CloudEvents topic is under the "jsonschema" folder.

When you are done adding the schemas to the topic you should have a view in the Confluent Cloud GUI similar to:

![EditSchema.png](Images%2FEditSchema.png)

Make sure the "topic" tag is updated in the projects "application.yaml" file to reflect the name of the topic that was created. You also may prefer to change the producer and consumer IDs, but this is not mandatory.

It is possible to download the schema from the registry using maven. if you use:

```
mvn schema-registry:download
```
the schemas are downloaded into the project under the "FromSchemaRegistry" folder and can be used to compare this to the JSON and Avro schemas that were loaded into the schema registry.

Next, it is necessary to create the POJOs that reference the stored JSON and Avro schemas. This can be done with:

```
mvn generate-sources
```

This will result in the POJOs being generated in the "target" folder, in this case under "io.confluent.heinz". The POJO for the Avro schema ends up under "io.confluent.heinz" based on the namespace in the schema. The JSON POJO end up in this package based on the definition for "targetPackage" tag in the maven plugin for JSON defined in the Maven "pom.xml" file.

Normally the POJOs generated from JSON schema are classes that are named based on the name of stored schema file. In this case the POJO is called "refSchema". This changed from the default since in the CloudEvent schema, there was another attribute added:

```
"javaType": "io.confluent.heinz.refSchema",
```

The maven plugin creates the POJO in the namespace followed by the name to be used for the schema POJO. This attribute is also necessary for proper deserialization when there are multiple schema in the same topic and is required for proper deserialization. 

To create and run the project simply execute:

```
mvn clean package
mvn spring-boot:run
```

At this point the test producer and consumer should be up and running. The project also contains a Rest Controller that is used to receive messages that are then forwarded to the Confluent producer. A consumer is  used to show the messages that are consumed.

A sample JSON message can be saved in a file called "data.json". An example of the message and the curl command used to send the message can be found in "PostRestData.zsh".

The REST POST via curl should generate console output showing the valid CloudEvent JSON message with the payload for the event found under "data". 

If everything was successful you should see output similar to:

```
2023-07-19T15:11:00.712-04:00  INFO 77466 --- [nio-9090-exec-2] i.c.h.c.restController                   : JSON REST POST Data -> {"customer_id":1234567890,"first_name":"Heinz","last_name":"Schaffner"} 
------------------------------------------ 
2023-07-19T15:11:00.713-04:00  INFO 77466 --- [nio-9090-exec-2] i.c.h.c.ConfluentSession                 : REST request data in sender method: {"customer_id":1234567890,"first_name":"Heinz","last_name":"Schaffner"}
Murmur Hash: 1442109826
Using key: 1234567892+Schaffner
Called partitioner 2 times
Murmur Hash: 1442109826
Using key: 1234567892+Schaffner
Called partitioner 3 times
2023-07-19T15:11:00.824-04:00  INFO 77466 --- [xSchemaProducer] .h.c.ConfluentSession$MyProducerCallback : AsynchronousProducer call Success:Sent to partition: 1 and offset: 5

2023-07-19T15:11:00.824-04:00  INFO 77466 --- [nio-9090-exec-2] i.c.h.c.ConfluentSession                 : Record sent to partition 1, with offset 5
------------------------------------------ 

+++++++++++++++++++++++++++++++
Consumed event from topic CloudEvents: key = {"client": "heinz57", "clientID": 1234567892} value = io.confluent.heinz.refSchema@4f26425b[data=io.confluent.heinz.JsonMsg@f03ee8f[customerId=1234567892,firstName=Heinz,lastName=Schaffner,additionalProperties={}],dataBase64=<null>,datacontenttype=<null>,dataschema=<null>,id=e18d26d9-7d6d-483c-9187-e0d396d67033,source=https://github.com/cloudevents/sdk-java/tree/main/examples/kafka,specversion=1.0,subject=<null>,time=<null>,type=confluent CE Sample,additionalProperties={}]
data customerID field: 1234567892
+++++++++++++++++++++++++++++++ 
```
From the Confluent Cloud GUI, you should see messages against the topic similar to:

```
{
  "data": {
    "customer_id": 1234567891,
    "first_name": "Heinz",
    "last_name": "Schaffner"
  },
  "id": "a6a5cd01-f466-4160-9b77-c20d6ccb1fac",
  "source": "https://github.com/cloudevents/sdk-java/tree/main/examples/kafka",
  "specversion": "1.0",
  "type": "confluent CE Sample"
}
```
A more complex CloudEvents message is possible, but for this test only the mandatory fields were used. However, the POJO that was generated from the CloudEvents schema supports all the CloudEvents attributes.

The data between the dashes is from the producer and between the pluses is from the consumer. 

You will notice some output with "Murmur Hash", "Using Key" and "Called Partitioner" that might look unfamiliar. 
In this project I added a customer partitioner that uses data from the record key and record value to determine the partition to use when writing the record rather than the default which would just use the 
the record key. There are two calls to the paritioner as part of the Confluent send, so this is not unusually to see the partioner counter increment twice for each published message. For those interested the partitioner
that was used is called "CustomAvroJsonPartitioner". It is possible to disable the 
custom partitioner by simply removing the following property definition from the "ConfluentSession" class

```
props.put("partitioner.class", CustomAvroJsonPartitioner.class);
```

## Additional Information

There are many options for interacting with Confluent Schema Registry. There are multiple options for using the maven plugin:

https://docs.confluent.io/platform/current/schema-registry/develop/maven-plugin.html

It is also possible to interact via the "confluent" CLI:

https://docs.confluent.io/confluent-cli/current/command-reference/schema-registry/index.html

The administrative REST API also supports Schema Registry

https://docs.confluent.io/platform/current/schema-registry/develop/api.html

A general API reference available as well:

https://docs.confluent.io/platform/current/schema-registry/develop/index.html

Details for serialization and JSON and JSON References is available here:

https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/serdes-json.html

