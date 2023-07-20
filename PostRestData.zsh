  curl -X POST -H "Content-Type: application/json" -d @data.json http://localhost:9090/test


#Sample of data.json file
  #{
    #	"first_name":"Heinz",
    #	"last_name":"Schaffner",
    #	"customer_id":1234567890
    #}

# KSQLSTREAM

#create stream HEINZSTREAM (
#  rowkey VARCHAR KEY) WITH (
#  KAFKA_TOPIC='jsonTopic.v1',
#  PARTITIONS=4,
#  VALUE_FORMAT='JSON_SR',
#  KEY_FORMAT='KAFKA');