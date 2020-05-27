package query2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import utility.ContinentDecoder;
import utility.IOUtility;
import utility.QueryUtility;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class Query2SparkSQL {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 2");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset2 = sparkContext.textFile(IOUtility.getDS2());

        // for benchmark purposes
        final long startTime = System.currentTimeMillis();

        // perform preprocessing
        JavaRDD<Tuple2<Double, CountryDataQuery2>> data = Query2Preprocessing.preprocessData(dataset2);

        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Double>> dailyData = data.flatMapToPair(
                tuple -> {
                    // initialization of result structure
                    // return values are 4-ples [continent, week, day of week, positive cases]
                    ArrayList<Tuple2<String, Tuple2<Tuple2<String, Integer>, Double>>> result = new ArrayList<>();
                    // detect continent
                    String continent = ContinentDecoder.detectContinent(tuple._2().getCoordinate());

                    Calendar currentDate = QueryUtility.getDataset2StartDate();

                    String week;

                    for (Double value : tuple._2.getCovidConfirmedCases()) {
                        // evaluate week
                        week = QueryUtility.getFirstDayOfTheWeek(currentDate.get(Calendar.WEEK_OF_YEAR),
                                currentDate.get(Calendar.YEAR));
                        // add to result structure
                        result.add(new Tuple2<>(continent, new Tuple2<>(new Tuple2<>(week,
                                currentDate.get(Calendar.DAY_OF_WEEK)), value)));
                        currentDate.add(Calendar.DATE, 1);
                    }

                    return result.iterator();

                }
        );

        SparkSession session = SparkSession
                .builder()
                .appName("Query 2 SparkSQL")
                .master("local")
                .getOrCreate();

        // create Spark SQL schema for dailyData
        Dataset<Row> dataFrame = createSchema(session, dailyData);

        // to access result as "SQL table"
        dataFrame.createOrReplaceTempView("query2");

        // perform day-by-day sum of values for every continent
        Dataset<Row> totalValues = session.sql("SELECT continent, week, sum(positive) AS positive " +
                "FROM query2 GROUP BY continent, week, day");

        // update "SQL table"
        totalValues.createOrReplaceTempView("query2");

        // evaluate statistics
        Dataset<Row> result = session.sql("SELECT continent, week, mean(positive) AS mean, " +
                "stddev(positive) AS stddev, min(positive) AS min, max(positive) AS max FROM query2 " +
                "GROUP BY continent, week ORDER BY continent, week");

        // necessary for correct performance evaluation
        result.collect();

        IOUtility.printTime(System.currentTimeMillis() - startTime);

        result.show((int) result.count());

        session.close();
        sparkContext.close();
    }

    private static Dataset<Row> createSchema(SparkSession session,
                                             JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Double>> data) {

        // generating schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("continent", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("week", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("day", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("positive", DataTypes.DoubleType, false));

        StructType schema = DataTypes.createStructType(fields);

        // convert RDD records to Rows
        JavaRDD<Row> rowRDD = data.zipWithIndex().map(element -> RowFactory.create(element._2(),
                element._1()._1(),
                element._1()._2()._1()._1(),
                element._1()._2()._1()._2(),
                element._1()._2()._2()));

        // apply schema to RDD and return
        return session.createDataFrame(rowRDD, schema);
    }
}
