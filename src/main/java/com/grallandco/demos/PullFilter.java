///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.grallandco.demos;
//
//import com.fasterxml.jackson.core.JsonFactory;
//
//import com.fasterxml.jackson.core.JsonParser;
//import com.fasterxml.jackson.core.JsonToken;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.JsonNodeFactory;
//import com.fasterxml.jackson.databind.util.JSONPObject;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
//import org.apache.sling.commons.json.JSONArray;
//
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
//import java.net.URL;
//import java.util.Properties;
//import java.lang.String;
//import java.util.concurrent.TimeUnit;
//
//public class PullFilter {
//
//
//    final JsonNodeFactory factory = JsonNodeFactory.instance;
//    static int numberOfQueryToGet = 3;
//    static String startingKey = "0";
//    static String theSource = "http://api.pathofexile.com/public-stash-tabs?id=";
//    static String whereToDump = "testdump/";
//    static String topic = "poeapi";
//
//    public static void main(String[] args) throws Exception {
//
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//
//        DataStream<String> stream = env.addSource(new SimpleStringGenerator());
//        stream.addSink(new FlinkKafkaProducer09<>("test", new SimpleStringSchema(), properties));
//
//        env.execute();
//    }
//
//    /**
//     * Simple Class to generate data
//     */
//    public static class SimpleStringGenerator implements SourceFunction<String> {
//        private static final long serialVersionUID = 119007289730474249L;
//        boolean running = true;
//        long i = 0;
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//            while(running) {
//                ctx.collect("FLINK-"+ (i++));
//                Thread.sleep(10);
//            }
//        }
//        @Override
//        public void cancel() {
//            running = false;
//        }
//    }
//
//    // wrapper for buffered reading of urls
//    public static BufferedReader wrapBR(URL thingToWrap) throws Exception{
//        return new BufferedReader(new InputStreamReader(thingToWrap.openStream()));
//    }
//
//
//    // given poe api url
//    // call api once
//    // return next key
//    // return api JsonToken of all stashes
//    public static Tuple2<String, JsonToken> pullMany(String theSource, String Key) throws Exception{
//
//
//        String nextChange = Key;
//        String urlContent = "";
//        //while(theNum < theLimit) {
//            TimeUnit.SECONDS.sleep(1);
//            URL url = new URL(theSource + nextChange);
//            System.out.println(url.toString());
//            BufferedReader br = wrapBR(url);
//
//            // System.out.println(urlContent);
//            urlContent = br.readLine();
//
//
//            //JsonToken fullContent = new Json(urlContent);
//
//            if (null!= urlContent){
//                JsonFactory f = new JsonFactory();
//                JsonParser allStash = f.createParser(urlContent);
//
//                try {
//                    nextChange = allStash.nextValue().asString();
//                    return new Tuple2<>(nextChange, allStash.nextValue());
//                }
//                catch (Exception oopsNoKey) {
//                    System.out.println("ooops invalid info pulled try again later...");
//                    //TimeUnit.SECONDS.sleep(10);
//                    return null;
//                }
//
//                // System.out.println("-------------------------------------------------------------------------");
//                // System.out.println("next:  " + nextChange);
//                // System.out.println("prev: " + previousChange);
//                // System.out.println("sleep 5 secs");
//
//                // move this section to another method
//                // check if new key, if not there are no new items
//            }
//        //} while
//        return null;
//    }
//
//    //check if nextchange is different from previous change
//    public static boolean isDifferent(String nextChange, String previousChange){
//        if(!nextChange.equals(previousChange)){
//            return true;
//        }else{
//            return false;
//        }
//    }
//
//    public static JsonToken getStash(JsonToken allStash){
//
//    }
//
//
//}
