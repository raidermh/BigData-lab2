package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.FlightEventCounter.countFlightPerHour;

public class SparkTest {

    final String testString1 = "1,6,Oct 26 11:24:09,DME,IST";
    final String testString2 = "2,6,Oct 26 17:24:09,DME,IST";
    final String testString3 = "3,6,Oct 26 15:24:09,DME,IST";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

    @Test
    public void testOneLog() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1));
        JavaRDD<Row> result = countFlightPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getInt(0) == 11;
        assert rowList.iterator().next().getString(1).equals("DME");
        assert rowList.iterator().next().getString(2).equals("IST]");
        assert rowList.iterator().next().getLong(3) == 1;
    }

    @Test
    public void testTwoLogsSameTime(){


        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString1));
        JavaRDD<Row> result = countFlightPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getInt(0) == 11;
        assert rowList.iterator().next().getString(1).equals("DME");
        assert rowList.iterator().next().getString(2).equals("IST]");
        assert rowList.iterator().next().getLong(3) == 2;
    }

    @Test
    public void testTwoLogsDifferentTime(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString3));
        JavaRDD<Row> result = countFlightPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getInt(0) == 11;
        assert firstRow.getString(1).equals("DME");
        assert firstRow.getString(2).equals("IST]");
        assert firstRow.getLong(3) == 1;

        assert secondRow.getInt(0) == 15;
        assert secondRow.getString(1).equals("DME");
        assert secondRow.getString(2).equals("IST]");
        assert secondRow.getLong(3) == 1;
    }

    @Test
    public void testThreeLogs(){

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> dudu = sc.parallelize(Arrays.asList(testString1, testString2, testString3));
        JavaRDD<Row> result = countFlightPerHour(ss.createDataset(dudu.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getInt(0) == 11;
        assert firstRow.getString(1).equals("DME");
        assert firstRow.getString(2).equals("IST]");
        assert firstRow.getLong(3) == 1;

        assert secondRow.getInt(0) == 15;
        assert secondRow.getString(1).equals("DME");
        assert secondRow.getString(2).equals("IST]");
        assert secondRow.getLong(3) == 1;

        assert thirdRow.getInt(0) == 17;
        assert thirdRow.getString(1).equals("DME");
        assert thirdRow.getString(2).equals("IST]");
        assert thirdRow.getLong(3) == 1;
    }

}
