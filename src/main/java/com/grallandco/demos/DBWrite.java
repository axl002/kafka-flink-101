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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.IOException;
import java.util.Properties;

public class DBWrite {
    //private static final RethinkDB r = RethinkDB.r;

    final static String STARTING_KEY = "0";
    final static String THE_SOURCE = "http://api.pathofexile.com/public-stash-tabs?id=";

    public static void main(String[] args) throws Exception {
        AmazonS3 s3;
        if(args.length == 2){
            String accessKey = args[0];
            String secretKey = args[1];
            s3 = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        //Properties outProps = new Properties();
        // setProperties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "zookeeper");
        props.setProperty("auto.offset.reset", "latest");
        //outProps.setProperty("bootstrap.servers", "localhost:9092");
        String topic = "poe3";

        //final Connection conn = r.connection().hostname("35.166.62.31").port(28015).connect();
        //conn.use("poeapi");

        //DataStream<String> stream = env.addSource(new FlinkKafkaConsumer09<String>(topic, new SimpleStringSchema(), props));

        env.execute();

    }

}

