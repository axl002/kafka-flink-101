/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.grallandco.demos;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class ReadFromKafka {

  public static Properties props;

  public static void main(String[] args) throws Exception {
    // create execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      Properties props = new Properties();
     // setProperties();
      props.setProperty("bootstrap.servers", "localhost:9092");
      props.setProperty("group.id", "zookeeper");
      String topic = "test";

    DataStream<String> stream = env
            .addSource(new FlinkKafkaConsumer09<>(topic, new SimpleStringSchema(), props));

    stream.map(new MapFunction<String, String>() {
      private static final long serialVersionUID = -6867736771747690202L;

      @Override
      public String map(String value) throws Exception {
        return "Stream Value: " + value;
      }
    }).print();

    env.execute();
  }


//  private static void setProperties(){
//
//    //String group = args[1].toString();
//    props.put("bootstrap.servers", "localhost:9092");
//    props.put("group.id", "zookeeper"); // need to test if zookeeper is required group, it works but do other groups work?
//    props.put("enable.auto.commit", "true");
//    props.put("auto.commit.interval.ms", "1000");
//    props.put("session.timeout.ms", "30000");
//    props.put("fetch.message.max.bytes","10000000");
//    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//  }


}
