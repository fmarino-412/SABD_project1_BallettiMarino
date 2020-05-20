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

        SparkSession session = SparkSession
                .builder()
                .appName("Query 2 SparkSQL")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataFrame = createSchema(session, dailyData);

        dataFrame.createOrReplaceTempView("query2");

        Dataset<Row> result = session.sql("select continent, date, mean(positive) as mean, min(positive) " +
                "as min, max(positive) as max, stddev(positive) as stddev from query2 group by continent, date " +
                "order by continent, date");

        IOUtility.printTime(System.currentTimeMillis() - startTime);

        result.show((int) result.count());

        session.close();
        sparkContext.close();
    }

    private static Dataset<Row> createSchema(SparkSession session, JavaPairRDD<String, Tuple2<String, Double>> data) {

        // Generating schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("continent", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("date", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("positive", DataTypes.DoubleType, false));

        StructType schema = DataTypes.createStructType(fields);

        // Convert RDD records to Rows
        JavaRDD<Row> rowRDD = data.zipWithIndex().map(element -> RowFactory.create(element._2(), element._1()._1(),
                element._1()._2()._1(), element._1()._2()._2()));

        // Apply schema to RDD and return
        return session.createDataFrame(rowRDD, schema);
    }
}
