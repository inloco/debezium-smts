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

## PostgresDebeziumGeopointMapping

This transform receives a standard debezium record and transforms two Float/Double fields to
a format that is accepted in ElasticSearch as geo_point(format="lat,lng").
The user has to pass the name of the streamed database's latitude and longitude fields and
also the name of the field he wants to output to.

## SetEventId

This transform adds a unique, UUID-generated field to represent a given event.
Its goal is supporting the automatic generation of unique ids for cases where they are not available in Debezium's source
and an outbox pattern is not being actively utilized. The name of the field may be passed as a configuration
in the Debezium source properties as `field`. If the given name already exists, the SMT ignores the given record.

## SetBeforeAndAfterName

This transform modifies the `namespace` and `name` fields for the `before` and `after` Debezium schemata.

Its usefulness lies in the fact that the default `SetSchemaMetadata` transform provided by Kafka Connect
does not account for inner values, and Debezium itself does not seem to offer an alternative to that.
This make it so the `before` and `after` namespaces are always set to the database configuration values
(e.g. database name, namespace, and table) with the `Value` name.

The transform's only configuration value is the new `name` for the schemata, which may be composed of a
dot-separated string that will in turn be converted as a package-like structure to generate the namespace and name for the records,
i.e. `com.my.app` turns into `{ "namespace": "com.my", "name": "app" }`.