spark-secure-kafka-app
============

### Introduction
This small app shows how to access data from a secure (Kerberized) Kafka cluster from Spark Streaming using [the new direct connector](http://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html) which uses the [new Kafka Consumer API](https://kafka.apache.org/documentation/#consumerconfigs). In order to use this app, you need to use Cloudera Distribution of Apache Kafka version 2.1.0 or later. And, you need to use Cloudera Distribution of Apache Spark 2 release 1 or later. Documentation for this integration can be found [here](https://www.cloudera.com/documentation/spark2/latest/topics/spark2_kafka.html).

Currently this example focuses on accessing Kafka securely via Kerberos. It assumes SSL (i.e. encryption over the wire) is configured for Kafka. It assumes that Kafka authorization (via Sentry, for example) is not being used. That can be setup separately.

### Build the app
To build, you need Python 2.7+, git and maven on the box.
Do a git clone of this repo and then run:
```
cd spark-secure-kafka-app
mvn clean package
```
Then, take the generated uber jar from `target/spark-secure-kafka-app-1.0-SNAPSHOT-jar-with-dependencies.jar` to the spark client node (where you are going to launch the query from). Let's assume you place this file in the home directory of this client machine.

### Running the app
#### Creating configuration
Before you run this app, you need to set up some JAAS configuration for Kerberos access. This particular configuration is inspired by that described in the [Apache Kafka documentation](https://kafka.apache.org/documentation/#security_kerberos_sasl_clientconfig). You also need to have access to the keytab needed for secure Kafka access.

We assume the client user's keytab is called `user.keytab` and is placed in the home directory on the client box. Let's create a file called `spark_jaas.conf` and place it in the home directory of the user as well with the JAAS conf:
```
# Change user.keytab to the keytab file name.
# Keep the beginning `./` infront of the keytab name. 
# Change principal to be the real principal below
cat << 'EOF' > spark_jaas.conf
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="./user.keytab"
    useTicketCache=false
    serviceName="kafka"
    principal="user@MY.DOMAIN.COM";
};
EOF
```

### spark-submit
Now run the follwing command:
```
# set num-executors, num-cores, etc. according to your needs.
# If simply testing, ok to leave the defaults as below
# Change references to user.keytab to the actual name of the keytab.
# If the keytab is not present in the current working directory,
# Change user.keytab#user.keytab to /full/path/to/user.keytab#user.keytab
# i.e. (only the filename is repeated after #, not the full path).
# If not using SSL, change the port 9093 below to the 9092.
# If not using SSL, change the last argument (true) to false.
SPARK_KAFKA_VERSION=0.10 spark2-submit \
  --num-executors 2 \
  --master yarn \
  --deploy-mode cluster \
  --files spark_jaas.conf#spark_jaas.conf,user.keytab#user.keytab \
  --driver-java-options "-Djava.security.auth.login.config=./spark_jaas.conf" \
  --class com.cloudera.spark.examples.DirectKafkaWordCount \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./spark_jaas.conf" \
  spark-secure-kafka-app-1.0-SNAPSHOT-jar-with-dependencies.jar \
  <kafka broker>:9093 \
  <topic> \
  true
```

### Generating some test data
While you run this app, you may want to generate some data in the topic Spark Streaming is reading from, and may want to view the word counts as the data is being generated. To generate data in the Kafka topic, you can use the `kafka-console-producer` using the following command:
```
# Create a Kafka topic
kafka-topics --create --zookeeper <zk node>:2181 --topic <topic> --partitions 4 --replication-factor 3

cd ~
# Generate the client.properties file which will be used by the console producer
# to select the appropriate security protocol and mechanism.
# If using SSL, also set `ssl.truststore.location` and `ssl.truststore.password` 
# properties appropriately in client.properties
# If not using SSL, change security.protocol's value to be SASL_PLAINTEXT (instead of SASL_SSL).
echo "security.protocol=SASL_SSL" >> client.properties
echo "sasl.kerberos.service.name=kafka" >> client.properties
```
Populate the following contents in a different JAAS conf, say `console.conf`:
```
# Change the /full/path/to/user.keytab below
# to the full path to the keytab.
# Change the principal name accordingly
cat << 'EOF' > console.conf
KafkaClient {
    com.sun.security.auth.module.Krb5LoginModule required
    useKeyTab=true
    storeKey=true
    keyTab="/full/path/to/user.keytab"
    useTicketCache=false
    serviceName="kafka"
    principal="user@MY.DOMAIN.COM";
}; 
EOF
```

```
# Run the console producer
# If not using SSL, change the port below to 9092
export KAFKA_OPTS="-Djava.security.auth.login.config=console.conf"
kafka-console-producer --broker-list <broker>:9093 --producer.config client.properties --topic <topic>

# Now, type in some words on the console, and close the producer.
```

### What's happening under the hood?
For consuming data via SASL/Kerberos, we pass on the JAAS configuration (`spark_jaas.conf`) to all executors. Along with this config, the keytab is also passed on to all executors.

These executors via the JAAS configuration know where the keytab is (in their working directory, since it was passed using `--files`). And, the driver (in the YARN cluster mode) and the executors then use the configured credentails to access Kafka via Kerberos tickets.

### What you should see
If all goes well, you should see counts of various words in every batch interval, in your spark streaming driver's stdout. To get driver's stdout (when using yarn cluster mode), please get the yarn logs using `yarn logs -applicationId <app ID>`. The `<app ID>` can be obtained through the console output on the client machine where `spark-submit` was launched from. In the retrieved logs, you would see something like:
```
-------------------------------------------
Time: 1494007312000 ms
-------------------------------------------
(word1,1)
(word2,1)

-------------------------------------------
Time: 1494007314000 ms
-------------------------------------------

```
