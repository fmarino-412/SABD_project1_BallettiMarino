package query1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

public class Query1Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> dataset1 = sparkContext.textFile("data/DS1.csv");

        // Transformations
        JavaPairRDD<Date, Tuple2<Integer, Integer>> pairs = dataset1.mapToPair(
                line -> {
                    // split csv line basing on the separator
                    String[] lineSplit = line.split(";");
                    // create the inner tuple as <cured, swabs>
                    Tuple2<Integer, Integer> innerTuple = new Tuple2<>(Integer.valueOf(lineSplit[1]),
                            Integer.valueOf(lineSplit[2]));
                    // split the date basing on the T character and save just the initial part (year, month, day)
                    String generalDate = (lineSplit[0].split("T"))[0];
                    // create the final tuple
                    return new Tuple2<>(new SimpleDateFormat("yyyy-MM-dd").parse(generalDate), innerTuple);
                }
        );

        JavaPairRDD<String, Tuple2<Integer, Integer>> weekPairs = pairs.mapToPair(
                tuple -> {
                    Calendar calendar = new GregorianCalendar(Locale.ITALIAN);
                    calendar.setTime(tuple._1());
                    String key = calendar.get(Calendar.WEEK_OF_YEAR) + "-" + calendar.get(Calendar.YEAR);
                    return new Tuple2<>(key, tuple._2());
                }
        ).cache();

        Map<String, Tuple2<Integer, Integer>> sumByWeek = weekPairs.reduceByKey(
                (tuple1, tuple2) -> new Tuple2<>(tuple1._1() + tuple2._1(), tuple1._2() + tuple2._2())
        ).collectAsMap();

        Map<String, Long> totals = weekPairs.countByKey();

        // Return structure creation
        Map<String, Double> averageCuredByWeek = new HashMap<>();
        Map<String, Double> averageSwabsByWeek = new HashMap<>();

        for (Map.Entry<String, Tuple2<Integer, Integer>> entry : sumByWeek.entrySet()) {
            String key = entry.getKey();
            Integer sumOfCured = entry.getValue()._1();
            Integer sumOfSwabs = entry.getValue()._2();
            averageCuredByWeek.put(key, (Double.valueOf(sumOfCured)/totals.get(key)));
            averageSwabsByWeek.put(key, (Double.valueOf(sumOfSwabs)/totals.get(key)));
        }

        System.out.println("Index\tWeek Number\tMean of cured\tMean of swabs");
        int i = 1;

        for (Map.Entry<String, Double> entry : averageCuredByWeek.entrySet()) {
            System.out.println("-------------------------------------------------------------------------------------");
            System.out.printf("%d): Week %s\t%f\t%f\n",
                    i, entry.getKey(), entry.getValue(), averageSwabsByWeek.get(entry.getKey()));
            System.out.println("-------------------------------------------------------------------------------------");
            i++;
        }

        // compute by key on master

        // Transformations
        // TODO: 1(PREPROCESSING): from cumulative to daily data
        // 2: from data as key to week number + year
        // 3: evaluate statistics

        // TODO: sparkContext.close();
        sparkContext.stop();
    }
}
