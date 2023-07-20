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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.heinz.avroMsgK;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CustomAvroJsonPartitioner implements Partitioner {
    private final Log logger = LogFactory.getLog(CustomAvroJsonPartitioner.class);
    private int actualPartition = 0;
    private int counter = 0;
    private int numPartitions = 0;

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        Object newKey = null;

        //get details about partitions
        if (counter == 0) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
            numPartitions = partitions.size();
            logger.info("Partition Size: " + numPartitions + " for topic: " + topic);
        }

        //only want to support records with keys
        if (keyBytes == null) {
            throw new InvalidRecordException("All messages should have a valid key");
        }
        if (Objects.nonNull(key)) {
            avroMsgK avroMsgKey = (avroMsgK) key;
            ObjectNode jsonMsg = (ObjectNode) value;
            newKey = avroMsgKey.getClientID();
            //newKey = newKey + "+" + ((ObjectNode) value).get("last_name");
            /*
            Iterator<String> it = ((ObjectNode)value).fieldNames();
            while(it.hasNext())
                System.out.println((Object) it.next());

             */
            JsonNode jsonNode = ((ObjectNode)value).get("payload");
            /*
            Iterator<String> jIt= jsonNode.fieldNames();
            while(jIt.hasNext()) {
                System.out.println(jIt.next());
            }
            System.out.println("Data: " + jsonNode.isContainerNode());
            Iterator<String> itt = jsonNode.fieldNames();
            while(itt.hasNext())
                System.out.println((Object) itt.next());

             */
            JsonNode dataNode = jsonNode.get("data");
            //System.out.println(dataNode.get("first_name").asText());
            String keyStr =  String.valueOf(newKey) + "+" + dataNode.get("last_name").asText() ;

            keyBytes = keyStr.getBytes();
            System.out.println("Murmur Hash: " + Utils.toPositive(Utils.murmur2(keyBytes)));
            System.out.println("Using key: " + keyStr);
            System.out.println("Called partitioner " + counter + " times");
            counter++;
        }
        return Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions);
    }

    @Override
    public void close() {
        System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=- Partitioner is closed");
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
