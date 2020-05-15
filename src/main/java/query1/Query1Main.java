package query1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.Config;
import utility.HdfsUtility;
import utility.QueryUtility;

import java.text.SimpleDateFormat;
import java.util.*;

public class Query1Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset1 = sparkContext.textFile(Config.getDS1());

        final long startTime = System.currentTimeMillis();

        // Transformations
        JavaPairRDD<Date, Tuple2<Integer, Integer>> pairs = dataset1.mapToPair(
                line -> {
                    // split csv line basing on the separator
                    String[] lineSplit = line.split(",");
                    // create the inner tuple as <cured, swabs>
                    Tuple2<Integer, Integer> innerTuple = new Tuple2<>(Integer.valueOf(lineSplit[1]),
                            Integer.valueOf(lineSplit[2]));
                    // split the date basing on the T character and save just the initial part (year, month, day)
                    String generalDate = (lineSplit[0].split("T"))[0];
                    // create the final tuple
                    return new Tuple2<>(new SimpleDateFormat("yyyy-MM-dd").parse(generalDate), innerTuple);
                }
        );

        JavaPairRDD<String, Tuple2<Double, Double>> averageDataByWeek = pairs.flatMapToPair(
                tuple -> {
                    List<Tuple2<String, Tuple2<Integer, Integer>>> result = new ArrayList<>();
                    Calendar calendar = new GregorianCalendar(Locale.ITALIAN);
                    calendar.setTime(tuple._1());
                    String key = QueryUtility.getFirstDayOfTheWeek(calendar.get(Calendar.WEEK_OF_YEAR),
                            calendar.get(Calendar.YEAR));
                    result.add(new Tuple2<>(key, tuple._2()));
                    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                        calendar.add(Calendar.WEEK_OF_YEAR, 1);
                        String key2 = QueryUtility.getFirstDayOfTheWeek(calendar.get(Calendar.WEEK_OF_YEAR),
                                calendar.get(Calendar.YEAR));
                        result.add(new Tuple2<>(key2, tuple._2()));
                    }
                    return result.iterator();
                }
        ).groupByKey(
        ).flatMapToPair(
                tuple -> {

                    ArrayList<Tuple2<String, Tuple2<Double, Double>>> result = new ArrayList<>();
                    ArrayList<Integer> cured = new ArrayList<>();
                    ArrayList<Integer> swabs = new ArrayList<>();

                    int elements = 0;

                    for (Tuple2<Integer, Integer> element : tuple._2()) {
                        cured.add(element._1());
                        swabs.add(element._2());
                        elements++;
                    }

                    cured.sort(Comparator.naturalOrder());
                    swabs.sort(Comparator.naturalOrder());

                    String firstWeekKey = QueryUtility.getFirstDayOfTheWeek(
                            QueryUtility.getDataset1StartDate().get(Calendar.WEEK_OF_YEAR),
                            QueryUtility.getDataset1StartDate().get(Calendar.YEAR));

                    if (!firstWeekKey.equals(tuple._1())) {
                        if (elements == 1) {
                            // week created with only a precedent week element
                            return result.iterator();
                        }

                        // we are not in the first week, there is precedent week data
                        // make week data independent from precedent weeks
                        for (int i = 1; i < elements; i++) {
                            cured.set(i, cured.get(i) - cured.get(0));
                            swabs.set(i, swabs.get(i) - swabs.get(0));
                        }
                        cured.remove(0);
                        swabs.remove(0);
                        elements = elements - 1;
                    }

                    // turn from cumulative to punctual data
                    for (int i = elements - 1; i > 0; i--) {
                        cured.set(i, cured.get(i) - cured.get(i-1));
                        swabs.set(i, swabs.get(i) - swabs.get(i -1));
                    }
                    Double avgCured = cured.stream().mapToInt(val -> val).average().orElse(0.0);
                    Double avgSwabs = swabs.stream().mapToInt(val -> val).average().orElse(0.0);
                    result.add(new Tuple2<>(tuple._1(), new Tuple2<>(avgCured, avgSwabs)));
                    return result.iterator();
                }
        ).sortByKey(true).cache();

        Map<String, Tuple2<Double, Double>> finalData = averageDataByWeek.collectAsMap();

        Config.printTime(System.currentTimeMillis() - startTime);

        System.out.println("Index\tWeek Start Day\t\t\t\t\tMean of cured\tMean of swabs");
        int i = 1;

        for (Map.Entry<String, Tuple2<Double, Double>> entry : finalData.entrySet()) {
            System.out.println("-------------------------------------------------------------------------------------");
            System.out.printf("%2d) Week starting at %s:\t\t%f\t\t%f\n",
                    i, entry.getKey(), entry.getValue()._1(), entry.getValue()._2());
            System.out.println("-------------------------------------------------------------------------------------");
            i++;
        }

        try {
            HdfsUtility.writeRDDToHdfs(Config.getOutputPathQuery1(), averageDataByWeek);
        } catch (Exception e) {
            System.err.println("This query output has already been saved in HDFS");
        }

        sparkContext.close();
    }
}
