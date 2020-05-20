package query1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.IOUtility;
import utility.QueryUtility;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Map;

public class Query1Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset1 = sparkContext.textFile(IOUtility.getDS1());

        final long startTime = System.currentTimeMillis();

        JavaPairRDD<String, Tuple2<Double, Double>> averageDataByWeek = Query1Preprocessing.preprocessData(dataset1)
            .groupByKey().flatMapToPair(
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
            ).sortByKey(true);

        // uncomment and add .cache() on the previous line to print result on console
        //printResult(averageDataByWeek.collectAsMap());

        IOUtility.printTime(System.currentTimeMillis() - startTime);

        IOUtility.writeRDDToHdfs(IOUtility.getOutputPathQuery1(), averageDataByWeek);

        sparkContext.close();
    }

    private static void printResult(Map<String, Tuple2<Double, Double>> finalData) {

        System.out.println("Index\tWeek Start Day\t\t\t\t\tMean of cured\tMean of swabs");
        int i = 1;

        for (Map.Entry<String, Tuple2<Double, Double>> entry : finalData.entrySet()) {
            System.out.println("-------------------------------------------------------------------------------------");
            System.out.printf("%2d) Week starting at %s:\t\t%f\t\t%f\n",
                    i, entry.getKey(), entry.getValue()._1(), entry.getValue()._2());
            System.out.println("-------------------------------------------------------------------------------------");
            i++;
        }

    }
}
