package query1;

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
import utility.IOUtility;
import utility.QueryUtility;

import java.util.ArrayList;
import java.util.List;

public class Query1SparkSQL {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Query 1");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        sparkContext.setLogLevel("ERROR");

        JavaRDD<String> dataset1 = sparkContext.textFile(IOUtility.getDS1());

        // for performance measurement
        final long startTime = System.currentTimeMillis();

        JavaPairRDD<String, Tuple2<Integer, Integer>> dailyData = Query1Preprocessing.preprocessData(dataset1)
                .groupByKey().flatMapToPair(
                        tuple -> {
                            // result structure initialization
                            ArrayList<Tuple2<String, Tuple2<Integer, Integer>>> result = new ArrayList<>();

                            // convert cumulative data to punctual
                            Tuple2<ArrayList<Integer>, ArrayList<Integer>> punctualData =
                                    QueryUtility.toPunctualData(tuple);

                            if (punctualData != null) {
                                // both arrays have same dimension, add data to result structure
                                for (int i = 0; i < punctualData._1().size(); i++) {
                                    result.add(new Tuple2<>(tuple._1(), new Tuple2<>(punctualData._1().get(i),
                                            punctualData._2().get(i))));
                                }
                            }
                            return result.iterator();
                        }
                );

        SparkSession session = SparkSession
                .builder()
                .appName("Query 1 SparkSQL")
                .master("local")
                .getOrCreate();

        // create the spark SQL schema for those results
        Dataset<Row> dataFrame = createSchema(session, dailyData);
        dataFrame.createOrReplaceTempView("query1");

        // SQL query to evaluate requested statistics
        Dataset<Row> result = session.sql("SELECT week, avg(cured) AS mean_cured, avg(swabs) AS mean_swabs " +
                "FROM query1 GROUP BY week ORDER BY week");

        // necessary for correct performance evaluation
        result.collect();

        IOUtility.printTime(System.currentTimeMillis() - startTime);

        // print query results
        result.show((int) result.count());

        //close both session and context
        session.close();
        sparkContext.close();
    }

    private static Dataset<Row> createSchema(SparkSession session, JavaPairRDD<String, Tuple2<Integer, Integer>> data) {

        // generating schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("id", DataTypes.LongType, false));
        fields.add(DataTypes.createStructField("week", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("cured", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("swabs", DataTypes.IntegerType, false));

        StructType schema = DataTypes.createStructType(fields);

        // convert RDD records to Rows
        JavaRDD<Row> rowRDD = data.zipWithIndex().map(element -> RowFactory.create(element._2(), element._1()._1(),
                element._1()._2()._1(), element._1()._2()._2()));

        // apply schema to RDD and return
        return session.createDataFrame(rowRDD, schema);
    }
}
