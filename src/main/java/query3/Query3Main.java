package query3;

import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import utility.Config;
import utility.QueryUtility;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public class Query3Main {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 3");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> dataset2 = sparkContext.textFile(Config.getDS2());

        //TODO: remove header with nifi
        JavaPairRDD<String, CountryDataQuery3> monthlyData = dataset2.filter(
                line -> !line.startsWith("Province")
        ).flatMapToPair(
                line -> {
                    List<Tuple2<String, CountryDataQuery3>> result = new ArrayList<>();
                    String[] splitted = line.split(",");
                    String name = splitted[0].equals("") ? splitted[1] : splitted[0];
                    List<Double> punctualData = QueryUtility.toPunctualData(Arrays.asList(splitted)
                            .subList(4, splitted.length));
                    Calendar currentDate = QueryUtility.getDataset2StartDate();
                    SimpleDateFormat format = new SimpleDateFormat("MM-yyyy");
                    String currentMonth = format.format(currentDate);
                    List<Double> monthlyPoints = new ArrayList<>();
                    for (Double singlePoint : punctualData) {
                        monthlyPoints.add(singlePoint);
                        currentDate.add(Calendar.DATE, 1);
                        if (!(format.format(currentDate)).equals(currentMonth)) {
                            result.add(new Tuple2<>(currentMonth, new CountryDataQuery3(name, currentMonth,
                                    monthlyPoints)));
                            monthlyPoints = new ArrayList<>();
                            currentMonth = format.format(currentDate);
                        }
                    }
                    if (!monthlyPoints.isEmpty()) {
                        result.add(new Tuple2<>(currentMonth, new CountryDataQuery3(name, currentMonth, monthlyPoints)));
                    }
                    return result.iterator();
                }
        );

        JavaPairRDD<String, Tuple2<Double, CountryDataQuery3>> monthlySlopes = monthlyData.mapToPair(
                tuple -> {
                    SimpleRegression regression = new SimpleRegression();
                    List<Double> values = tuple._2().getValues();
                    for (int i = 0; i < values.size(); i++) {
                        regression.addData(i, values.get(i));
                    }
                    tuple._2().setSlope(regression.getSlope());
                    return new Tuple2<>(tuple._1(), new Tuple2<>(tuple._2().getSlope(), tuple._2()));
                }
        );

        //monthlySlopes.groupByKey().mapTo
    }


}
