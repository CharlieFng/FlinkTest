package tableAndsql;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class TableApiImpl {

    public static void main(String[] args) throws Exception
    {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        /* create table from csv */
        TableSource tableSrc = CsvTableSource.builder()
                .path("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/inputs/avg")
                .fieldDelimiter(",")
                .field("date", Types.STRING)
                .field("month", Types.STRING)
                .field("category", Types.STRING)
                .field("product", Types.STRING)
                .field("profit", Types.INT)
                .build();

        tableEnv.registerTableSource("CatalogTable", tableSrc);

        Table catalog = tableEnv.scan("CatalogTable");

        /* querying with Table API */
        Table order20 = catalog
                .filter(" category === 'Category5'")
                .groupBy("month")
                .select("month, profit.sum as sum")
                .orderBy("sum");


        DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);

        order20Set.writeAsText("/Users/fengs4/Desktop/Udemy/Flink/src/main/resources/outputs/avg-table-res.txt");
        //tableEnv.toAppendStream(order20, Row.class).writeAsText("/home/jivesh/table");
        env.execute("Table API Example");
    }

    public static class Row1
    {
        public String month;
        public Integer sum;

        public Row1(){}

        public String toString()
        {
            return month + "," + sum;
        }
    }

}
