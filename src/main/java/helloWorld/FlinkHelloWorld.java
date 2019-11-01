package helloWorld;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Minimum Flink program!
 */
public class FlinkHelloWorld {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> greetings = see.fromCollection(Lists.newArrayList("Hello", "Apache", "Flink"));

        greetings.print();

        see.execute();
    }
}
