import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class ClickAnalysis {


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> data = env.readTextFile("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/inputs/ip-data.txt");

        // click data keyed by website
        DataStream<Tuple2<String, String>> keyedData = data
                .map(value -> {
                    String[] words = value.split(",");
                    return Tuple2.of(words[4], value);
                }).returns(Types.TUPLE(Types.STRING,Types.STRING));

        // US click stream only
        DataStream<Tuple2<String, String>> usStream = keyedData
                .filter(value -> {
                    String country = value.f1.split(",")[3];
                    return !country.equals("US");
                });

        // total number of clicks on every website in US
        DataStream<Tuple2<String, Integer>> clicksPerWebsite = usStream
                .map(value -> Tuple3.of(value.f0, value.f1, 1))
                .returns(Types.TUPLE(Types.STRING,Types.STRING,Types.INT))
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .sum(2)
                .map(value-> Tuple2.of(value.f0, value.f2))
                .returns(Types.TUPLE(Types.STRING,Types.INT));

        clicksPerWebsite.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/outputs/clicks_per_web.txt", FileSystem.WriteMode.OVERWRITE);


        // website with max clicks
        DataStream<Tuple2<String, Integer>> maxClicks =	 clicksPerWebsite
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .maxBy(1);

        maxClicks.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/outputs/max_clicks.txt", FileSystem.WriteMode.OVERWRITE);


        // website with min clicks
        DataStream<Tuple2<String, Integer>> minClicks = clicksPerWebsite
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .minBy(1);

        minClicks.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/outputs/min_clicks.txt", FileSystem.WriteMode.OVERWRITE);


        DataStream<Tuple2<String, Integer>> avgTimeWebsite = usStream
            .map(value -> {
                    int timeSpent = Integer.parseInt(value.f1.split(",")[5]);
                    return Tuple3.of(value.f0, 1, timeSpent);
            }).returns(Types.TUPLE(Types.STRING,Types.INT,Types.INT))
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
            .reduce((v1,v2) -> Tuple3.of(v1.f0, v1.f1+v2.f1, v1.f2+v2.f2))
            .map(value -> Tuple2.of(value.f0, (value.f2/value.f1)))
            .returns(Types.TUPLE(Types.STRING,Types.INT));

        avgTimeWebsite.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/outputs/avg_per_web.txt", FileSystem.WriteMode.OVERWRITE);

        // distinct users on each website
        DataStream<Tuple2<String, Integer>> usersPerWebsite = usStream
            .keyBy(0)
            .flatMap(new DistinctUsers());

        usersPerWebsite.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/outputs/distinct_users.txt", FileSystem.WriteMode.OVERWRITE);

        // execute program
        env.execute("Streaming Click");
    }

    public static class DistinctUsers extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {

        private transient ListState<String> usersState;

        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> out) throws Exception {

            usersState.add(input.f1);

            HashSet<String> distinctUsers = new HashSet<>();
            for (String user : usersState.get()) {
                distinctUsers.add(user);
            }
            out.collect(Tuple2.of(input.f0, distinctUsers.size()));
        }

        public void open(Configuration conf) {
            ListStateDescriptor<String> desc = new ListStateDescriptor<>("users_state", BasicTypeInfo.STRING_TYPE_INFO);
            usersState = getRuntimeContext().getListState(desc);
        }
    }

}
