package query2;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.*;

import java.util.*;

public class Query2Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset2 = sparkContext.textFile(Config.getDS2());

        final long startTime = System.currentTimeMillis();

        // convert data in RDD
        JavaPairRDD<Double, CountryDataQuery2> data = dataset2.mapToPair(
                line -> {
                    String[] splitted = line.split(",");
                    String key = splitted[0].equals("") ? splitted[1] : splitted[0];
                    GeoCoordinate geoCoordinate = new GeoCoordinate(splitted[2], splitted[3]);
                    CountryDataQuery2 countryData = new CountryDataQuery2(geoCoordinate,
                            Arrays.asList(splitted).subList(4,splitted.length), key);

                    SimpleRegression regression = new SimpleRegression();
                    List<Double> values = countryData.getCovidConfirmedCases();
                    for (int i = 0; i < values.size(); i++) {
                        regression.addData(i, values.get(i));
                    }
                    return new Tuple2<>(regression.getSlope(), countryData);
                });

        JavaPairRDD<String, List<Double>> continents = data.sortByKey(false).zipWithIndex()
                .filter(xi -> xi._2 < 100).keys().flatMapToPair(
                        tuple -> {
                            ArrayList<Tuple2<String, List<Double>>> result = new ArrayList<>();
                            String keyHeader = ContinentDecoder.detectContinent(tuple._2().getCoordinate()) + " - ";

                            Calendar currentDate = QueryUtility.getDataset2StartDate();
                            // work on different years too being sequential and taking care of just two following values
                            int currentWeekNumber = currentDate.get(Calendar.WEEK_OF_YEAR);
                            List<Double> weeklyValues = new ArrayList<>();
                            for (Double value : tuple._2().getCovidConfirmedCases()) {
                                if (currentWeekNumber != currentDate.get(Calendar.WEEK_OF_YEAR)) {
                                    currentDate.add(Calendar.WEEK_OF_YEAR, -1);
                                    result.add(new Tuple2<>(keyHeader +
                                            QueryUtility.getFirstDayOfTheWeek(currentDate.get(Calendar.WEEK_OF_YEAR),
                                                    currentDate.get(Calendar.YEAR)), weeklyValues));
                                    currentDate.add(Calendar.WEEK_OF_YEAR, 1);
                                    weeklyValues = new ArrayList<>();
                                    currentWeekNumber = currentDate.get(Calendar.WEEK_OF_YEAR);
                                }
                                weeklyValues.add(value);
                                currentDate.add(Calendar.DATE, 1);
                            }
                            // end of data, add last batch if present
                            if (!weeklyValues.isEmpty()) {
                                if (weeklyValues.size() == 7) {
                                    // the week is completed so the current date points to the next monday,
                                    // return to the right week
                                    currentDate.add(Calendar.DATE, -1);
                                }
                                result.add(new Tuple2<>(keyHeader +
                                        QueryUtility.getFirstDayOfTheWeek(currentDate.get(Calendar.WEEK_OF_YEAR),
                                                currentDate.get(Calendar.YEAR)), weeklyValues));

                            }
                            return result.iterator();
                        }
                ).reduceByKey(
                        (x, y) -> {
                            // same x and y list length due to dataset update rules
                            List<Double> sum = new ArrayList<>();
                            for (int i = 0; i < x.size(); i++) {
                                sum.add(x.get(i) + y.get(i));
                            }
                            return sum;
                        }
                );

        JavaPairRDD<String, List<Double>> statistics = continents.mapToPair(
                        tuple -> {
                            double weeklyMean = 0.0;
                            double weeklyStdDev = 0.0;

                            int weekLength = tuple._2().size();

                            // compute max and min
                            double weeklyMax = Collections.max(tuple._2());
                            double weeklyMin = Collections.min(tuple._2());

                            // compute mean
                            for (Double value : tuple._2()) {
                                weeklyMean += value;
                            }
                            weeklyMean = weeklyMean / weekLength;

                            // compute standard deviation
                            for (Double value : tuple._2()) {
                                weeklyStdDev += Math.pow((value - weeklyMean), 2);
                            }
                            weeklyStdDev = weeklyStdDev / weekLength;

                            List<Double> result = Arrays.asList(weeklyMean, weeklyStdDev, weeklyMin, weeklyMax);

                            return new Tuple2<>(tuple._1(), result);
                        }
                );

        // TODO: remove sort by key
        JavaPairRDD<String, List<Double>> orderedStatistics = statistics.sortByKey(true).cache();
        Config.printTime(System.currentTimeMillis() - startTime);

        List<Tuple2<String, List<Double>>> orderedResult = orderedStatistics.collect();

        // TODO: remove in future
        System.out.println("Index\tWeek Start Day\t\t\tMean\tStandard Deviation\tMinimum\tMaximum");
        int i = 1;
        for (Tuple2<String, List<Double>> element : orderedResult) {

            System.out.println("-------------------------------------------------------------------------------------");

            System.out.printf("%2d) %s:\t\t%f\t\t%f\t\t%f\t\t%f\n",
                    i, element._1(), element._2().get(0), element._2().get(1), element._2().get(2), element._2().get(3));

            System.out.println("-------------------------------------------------------------------------------------");
            i++;
        }

        HdfsUtility.writeRDDToHdfs(Config.getOutputPathQuery2(), orderedStatistics);

        sparkContext.close();
    }
}
