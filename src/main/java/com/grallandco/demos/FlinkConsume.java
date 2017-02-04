package com.grallandco.demos;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;


import java.io.IOException;
import java.util.Properties;

// current function 02-01-2017
// filters incoming json to only be from standard league items priced in chaos orbs or exalts
// performs streaming wordcount
public class FlinkConsume{


    public static Properties props;

    public static void main(String[] args) throws Exception {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        Properties outProps = new Properties();
        // setProperties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "zookeeper");
        props.setProperty("auto.offset.reset","latest");
        outProps.setProperty("bootstrap.servers", "localhost:9092");
        String topic = "poe2";
        String topicOut = "poe3";



        // source
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer09<String>(topic, new SimpleStringSchema(), props));

        // redis setup

//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
//                .setHost("172.31.2.233")
//                .setPort(30001)
//                .build();
                //.setNodes(new HashSet<InetSocketAddress>(Arrays.asList(new InetSocketAddress(30001)))).build();


        //stream.addSink(new RedisSink<String>(conf, new LUTRedisMapper()));


        // extract and return stream with unique key of sellerID+ItemID+price and count of 1
        stream.map(new MapFunction<String, Tuple3<String, ObjectNode, Integer>>() {
            @Override
            public Tuple3<String, ObjectNode, Integer> map(String input) throws Exception {
                ObjectNode on = parseJsonMutable(input);
                String key = on.get("lastSeller").asText() +
                        on.get("itemID").asText() +
                        on.get("note").asText();
                return Tuple3.of(key, on, 1);
            }
        })
                // keyBy unique key
                .keyBy(0)
                // aggregate and return count of unique key for de-duping
                .fold(Tuple3.of("0", new ObjectNode(null), 0), new FoldFunction<Tuple3<String, ObjectNode, Integer>, Tuple3<String, ObjectNode, Integer>>() {
            @Override
            public Tuple3<String, ObjectNode, Integer> fold(Tuple3<String, ObjectNode, Integer> old, Tuple3<String, ObjectNode, Integer> current) throws Exception {
                return Tuple3.of(current.f0, current.f1, current.f2+old.f2);
            }
        })
                // if the counter of unique id is less than or equal 1, then we have a new item, let it pass
                .filter(new FilterFunction<Tuple3<String, ObjectNode, Integer>>() {
            @Override
            public boolean filter(Tuple3<String, ObjectNode, Integer> inputWithDuplicationCounter) throws Exception {
                return inputWithDuplicationCounter.f2<=1;
            }
        })
                // revert back to raw stream, now without duplicates
                .map(new MapFunction<Tuple3<String,ObjectNode,Integer>, ObjectNode>() {
            @Override
            public ObjectNode map(Tuple3<String, ObjectNode, Integer> noDupeInput) throws Exception {
                return noDupeInput.f1;
            }
        });

        //filter to items with b/o priced in exalts or chaos
        stream.filter(new FilterFunction<String>() {
            private static final long serialVersionUID = -6867736771747690202L;
            @Override
            public boolean filter(String value) throws Exception {
                // create json
                String outString = "nothing here";
                JsonNode on = parseJson(value);
                try {
                    outString = on.get("note").asText();
                    return outString.contains("chaos") || outString.contains("exa");
                }catch (NullPointerException npe){
                    return false;
                }
            }
            // filter to standard
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                String outString = "nothing here";
                JsonNode on = parseJson(s);

                try {
                    outString = on.get("league").asText();
                    return outString.contains("Standard");
                }catch (NullPointerException npe){
                    return false;
                }
            }
        })
//                .map(new MapFunction<String, String>() {
//            private static final long serialVersionUID = -6867736771747690202L;
//
//            @Override
//            public String map(String value) throws Exception {
//                String outString = "nothing here";
//                ObjectMapper mapper = new ObjectMapper();
//                JsonNode rootNode = null;
//                if(null != value){
//                    rootNode = mapper.readTree(value);
//                }
//                try {
//                    outString = rootNode.get("name").asText() + " | " + rootNode.get("typeLine").asText() + rootNode.get("note").asText();
//                    return "Stream Value: " + outString;
//                }catch (NullPointerException npe){
//                    return "null value";
//                }
//            }
//        })
                //.addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));//.print();;

                //DataStream<String> dankStream = stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                Tuple2<String, Integer> outString = new Tuple2<>();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode rootNode = null;
                if(null != value){
                    rootNode = mapper.readTree(value);
                }
                try {
                    outString.setFields(rootNode.get("name").asText() + " | " + rootNode.get("typeLine").asText(), 1);
                    return outString;
                }catch (NullPointerException npe){
                    return null;
                }

            }
        })
                .keyBy(0)
                // count number of items using fold
                .fold(Tuple2.of("0", 0), new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> fold(Tuple2<String, Integer> old, Tuple2<String, Integer> current) throws Exception {
//                        int newCount = old.f1 +current.f1;
                        return Tuple2.of(current.f0, old.f1 +current.f1);

                    }
                }).map(new MapFunction< Tuple2<String, Integer>, String>() {
                    @Override
                    public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return "~foo~" + stringIntegerTuple2.f0 + "~doo~" + Integer.toString(stringIntegerTuple2.f1) + "~yoo~";
                    }
                })
                .addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));





        // older stuff
//        DataStream<String> morphStream = stream.map(new MapFunction<String, String>() {
//            private static final long serialVersionUID = -6867736771747690202L;
//
//            public String map(String value) throws Exception {
//
//                return "Stream Value: " + value;
//            }
//        }).addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));//.print();


        //morphStream.addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));
        // sink
        //stream.addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));

        env.execute();
    }


    // for editing json operations
    private static ObjectNode parseJsonMutable(String raw) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = null;
        if(null != raw){
            rootNode = mapper.readTree(raw);
        }
        return (ObjectNode) rootNode;
    }

    // for read only json operations
    private static JsonNode parseJson(String raw) throws IOException{
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = null;
        if(null != raw){
            rootNode = mapper.readTree(raw);
        }
        return rootNode;
    }

}
