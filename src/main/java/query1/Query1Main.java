package query1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.IOUtility;
import utility.QueryUtility;

import java.util.ArrayList;
import java.util.Map;

public class Query1Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset1 = sparkContext.textFile(IOUtility.getDS1());

        // for performance measurement
        final long startTime = System.currentTimeMillis();

        JavaPairRDD<String, Tuple2<Double, Double>> averageDataByWeek = Query1Preprocessing.preprocessData(dataset1)
            // obtain [Week start day as string, [List of cured in the week, List of swabs in the week]]
            .groupByKey().flatMapToPair(
                    tuple -> {
                        ArrayList<Tuple2<String, Tuple2<Double, Double>>> result = new ArrayList<>();
                        // convert to punctual data
                        Tuple2<ArrayList<Integer>, ArrayList<Integer>> punctualData =
                                QueryUtility.toPunctualData(tuple);

                        if (punctualData != null) {
                            Double avgCured = punctualData._1().stream().mapToInt(val -> val).average()
                                    .orElse(0.0);
                            Double avgSwabs = punctualData._2().stream().mapToInt(val -> val).average()
                                    .orElse(0.0);
                            result.add(new Tuple2<>(tuple._1(), new Tuple2<>(avgCured, avgSwabs)));
                        }
                        // if null there was a single value corresponding to the previous week,
                        // an empty array will be returned
                        return result.iterator();
                    }
            ).sortByKey(true).cache();

        // without console printing result this line is not needed, although it was added for benchmark purposes
        Map<String, Tuple2<Double, Double>> finalResult = averageDataByWeek.collectAsMap();

        // uncomment next line to print result on console
        //printResult(finalResult);

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
