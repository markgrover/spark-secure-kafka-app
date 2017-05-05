/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.examples

import org.apache.kafka.clients.consumer.ConsumerRecord

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies, KafkaUtils}
import org.apache.spark.streaming._

object DirectKafkaWordCount {
    def main(args: Array[String]) {
      if (args.length < 2) {
        System.err.println(s"""
                              |Usage: DirectKafkaWordCount <brokers> <topics>
                              |  <brokers> is a list of one or more Kafka brokers
                              |  <topics> is a list of one or more kafka topics to consume from
                              |  <ssl> true if using SSL, false otherwise.
                              |
        """.stripMargin)
        System.exit(1)
      }

      val Array(brokers, topics, ssl) = args

      // Create context with 2 second batch interval
      val sparkConf = new SparkConf().setAppName("DirectKafkaWordCount")
      val ssc = new StreamingContext(sparkConf, Seconds(2))
      val isUsingSsl = ssl.toBoolean

      // Create direct kafka stream with brokers and topics
      val topicsSet = topics.split(",").toSet
      val commonParams = Map[String, Object](
        "bootstrap.servers" -> brokers,
        "security.protocol" -> (if (isUsingSsl) "SASL_SSL" else "SASL_PLAINTEXT"),
        "sasl.kerberos.service.name" -> "kafka",
        "auto.offset.reset" -> "earliest",
        "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
        "group.id" -> "default",
        "enable.auto.commit" -> (false: java.lang.Boolean)
      )

      val additionalSslParams = if (isUsingSsl) {
        Map(
          "ssl.truststore.location" -> "/etc/cdep-ssl-conf/CA_STANDARD/truststore.jks",
          "ssl.truststore.password" -> "cloudera"
        )
      } else {
        Map.empty
      }

      val kafkaParams = commonParams ++ additionalSslParams

      val messages: InputDStream[ConsumerRecord[String, String]] =
        KafkaUtils.createDirectStream[String, String](
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
        )

      // Get the lines, split them into words, count the words and print
      val lines = messages.map(_.value())
      val words = lines.flatMap(_.split(" "))
      val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
      wordCounts.print()

      // Start the computation
      ssc.start()
      ssc.awaitTermination()
    }
}
