package query2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.ContinentDecoder;
import utility.IOUtility;
import utility.QueryUtility;

import java.util.ArrayList;
import java.util.Calendar;

public class Query2SparkSQL {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset2 = sparkContext.textFile(IOUtility.getDS2());

        final long startTime = System.currentTimeMillis();

        JavaRDD<Tuple2<Double, CountryDataQuery2>> data = Query2Preprocessing.preprocessData(dataset2);

        JavaPairRDD<String, Tuple2<String, Double>> dailyData = data.flatMapToPair(
                tuple -> {

                    ArrayList<Tuple2<String, Tuple2<String, Double>>> result = new ArrayList<>();
                    String continent = ContinentDecoder.detectContinent(tuple._2().getCoordinate());

                    Calendar currentDate = QueryUtility.getDataset2StartDate();

                    for (Double value : tuple._2.getCovidConfirmedCases()) {
                        result.add(new Tuple2<>(continent, new Tuple2<>(
                                QueryUtility.getFirstDayOfTheWeek(currentDate.get(Calendar.WEEK_OF_YEAR),
                                        currentDate.get(Calendar.YEAR)), value)));
                        currentDate.add(Calendar.DATE, 1);
                    }

                    return result.iterator();

                }
        );
    }
}
