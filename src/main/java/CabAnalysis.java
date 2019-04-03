import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;


public class CabAnalysis {


    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple8<String, String, String, String, Boolean, String, String, Integer>> data = env.readTextFile("/Users/fengs4/Desktop/Udemy/Flink/src/test/resources/inputs/cab-flink.txt")
                .map( value -> {
                    String[] words = value.split(",");
                    Boolean status = false;
                    if (words[4].equalsIgnoreCase("yes"))
                        status = true;
                    if (status)
                        return Tuple8.of(words[0], words[1], words[2], words[3], status, words[5], words[6], Integer.parseInt(words[7]));
                    else
                        return Tuple8.of(words[0], words[1], words[2], words[3], status, words[5], words[6], 0);
                })
                .returns(Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.BOOLEAN,Types.STRING,Types.STRING,Types.INT))
                .filter(value -> value.f4);

        // most popular destination
        DataSet<Tuple8<String, String, String, String, Boolean, String, String, Integer>> popularDest = data.groupBy(6).sum(7).maxBy(7);
        popularDest.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/test/resources/outputs/popularDes.txt", FileSystem.WriteMode.OVERWRITE);

        // avg. passengers per trip source: place to pickup most passengers
        DataSet<Tuple2<String, Double>> avgPassPerTrip = data
                .map(value -> Tuple3.of(value.f5, value.f7, 1))
                .returns(Types.TUPLE(Types.STRING,Types.INT,Types.INT))
                .groupBy(0)
                .reduce((v1, v2) -> Tuple3.of(v1.f0, v1.f1+v2.f1, v1.f2+v2.f2))
                .map(value -> Tuple2.of(value.f0, ((value.f1*1.0)/value.f2)))
                .returns(Types.TUPLE(Types.STRING,Types.DOUBLE));
        avgPassPerTrip.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/test/resources/outputs/avg_passengers_per_trip.txt", FileSystem.WriteMode.OVERWRITE);

        // avg. passengers per driver: popular/efficient driver
        DataSet<Tuple2<String, Double>> avgPassPerDriver = data
                .map(value -> Tuple3.of(value.f3, value.f7, 1))
                .returns(Types.TUPLE(Types.STRING,Types.INT,Types.INT))
                .groupBy(0)
                .reduce((v1, v2) -> Tuple3.of(v1.f0, v1.f1+v2.f1, v1.f2+v2.f2))
                .map(value -> Tuple2.of(value.f0, ((value.f1*1.0)/value.f2)))
                .returns(Types.TUPLE(Types.STRING,Types.DOUBLE));

        avgPassPerDriver.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/test/resources/outputs/avg_passengers_per_driver.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute("Cab Analysis");

    }


}
