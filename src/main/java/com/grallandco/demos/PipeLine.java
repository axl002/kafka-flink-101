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
import java.util.Random;
// current function 02-01-2017
// filters out duplicates
// filters incoming json to only be from standard league items priced in chaos orbs or exalts
// performs streaming itemcount
// randomly assigns price according to gaussian dist
public class PipeLine{

    public static final Random myRNG = new Random();
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
                // other filtering
                .filter(new FilterFunction<Tuple3<String, ObjectNode, Integer>>() {
            @Override
            public boolean filter(Tuple3<String, ObjectNode, Integer> input) throws Exception {
                String outString = input.f1.get("note").asText();

                try {
                    return outString.contains("chaos") || outString.contains("exa");
                }
                catch (NullPointerException npe){
                    npe.printStackTrace();
                    return false;
                }
            }
        })
                .filter(new FilterFunction<Tuple3<String, ObjectNode, Integer>>() {
            @Override
            public boolean filter(Tuple3<String, ObjectNode, Integer> input) throws Exception {
                String outString = input.f1.get("league").asText();
                try{
                    return outString.contains("Standard");
                }
                catch (NullPointerException npe){
                    npe.printStackTrace();
                    return false;
                }
            }
        })
                // map to new key of type Line + item name
                .map(new MapFunction<Tuple3<String,ObjectNode,Integer>, Tuple3<String,ObjectNode,Integer>>() {
                    @Override
                    public Tuple3<String,ObjectNode,Integer> map(Tuple3<String, ObjectNode, Integer> noDupeInput) throws Exception {
                        String key = noDupeInput.f1.get("name").asText() + noDupeInput.f1.get("typeLine").asText();
                        return Tuple3.of(key, noDupeInput.f1, 1);
                    }
                }).keyBy(0)
                // item count using fold
                .fold(Tuple3.of("0", new ObjectNode(null), 0), new FoldFunction<Tuple3<String, ObjectNode, Integer>, Tuple3<String, ObjectNode, Integer>>() {
            @Override
            public Tuple3<String, ObjectNode, Integer> fold(Tuple3<String, ObjectNode, Integer> old, Tuple3<String, ObjectNode, Integer> current) throws Exception {
                return Tuple3.of(current.f0,current.f1,current.f2+old.f2);
            }
            // revert back to single string stream
        }).map(new MapFunction<Tuple3<String,ObjectNode,Integer>, String>() {
            @Override
            public String map(Tuple3<String, ObjectNode, Integer> filteredInput) throws Exception {
                ObjectNode on = filteredInput.f1;
                on.put("count", filteredInput.f2);
                on.put("price", parseValue(on.get("note").asText()));
                return on.toString();
            }
        }).addSink(new FlinkKafkaProducer09<String>(topicOut, new SimpleStringSchema(), outProps));

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

    private static double parseValue(String note){
        String dankNote = note.toLowerCase();
        // check if buyout
        try {
            if(dankNote.contains("~b/o".toLowerCase())){
                if (dankNote.contains("chaos")){
                    return Double.parseDouble(dankNote.replaceAll("[^0-9]+", ""))/72;
                }
                else if (dankNote.contains("exa")){
                    return Double.parseDouble(dankNote.replaceAll("[^0-9]+", ""));
                }
                else {
                    return -1;
                }
            }
            // problem formatting number skip and return -3
        }catch (NumberFormatException nfe){
            return -3;
        }

        return -2;
    }

}
