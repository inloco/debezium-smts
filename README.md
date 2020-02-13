# debezium-smts

This project contains custom SMTs to be used in Debezium tasks for reading
database rows and producing to Kafka.

For these to be used in a task, first the JAR should be generated:

```
./gradlew jar
```

And then, it has to be copied to the Kafka Connect directory of the deployment.

##  SetDebeziumRecordPartition

This transform makes it so records that have the same given key field will
always be in the same Kafka Partition. This is especially useful for cases
when updates for the same entity has to be in the same partition.

## SetTombstoneRecord

Tombstone (null-valued messages) are the only way to deleted old messages from
Kafka. This transform makes it so that records that have the null value in a
specified value field will produce null-valued messages.
