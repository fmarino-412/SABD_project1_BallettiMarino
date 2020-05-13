package utility;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import query3.CountryDataQuery3;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusteringUtility {

    private static final boolean NAIVE = Boolean.FALSE;
    private static final Integer CLUSTERS = 4;
    private static final Integer ITERATION = 20;
    private static final String INITIALIZATION_MODE = "random";
    private static final Integer RUNS = 3;

    public void clusteringNaive() {

    }

    public static void clusteringMLlib(JavaPairRDD<String, List<Tuple2<Double, CountryDataQuery3>>> data, String month) {

        JavaPairRDD<Double, String> toCluster = data.flatMapToPair(
                tuple -> {
                    ArrayList<Tuple2<Double, String>> result = new ArrayList<>();
                    for (Tuple2<Double, CountryDataQuery3> elem : tuple._2()) {
                        result.add(new Tuple2<>(elem._1(), elem._2().getName()));
                    }
                    return result.iterator();
                }
        ).cache();

        JavaRDD<Vector> values = toCluster.map(
                tuple -> Vectors.dense(tuple._1())
        );

        KMeansModel model = KMeans.train(values.rdd(), CLUSTERS, ITERATION, INITIALIZATION_MODE, RUNS);
        List<Tuple2<Double, String>> toPredict = toCluster.collect();

        // initialization of result structure
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        for (int i = 0; i < CLUSTERS; i++) {
            result.add(new ArrayList<>());
        }

        for (Tuple2<Double, String> elem : toPredict) {
            int clusterIndex = model.predict(Vectors.dense(elem._1()));
            result.get(clusterIndex).add(elem._2());
        }

        System.out.println("Results for: " + month);
        for (ArrayList<String> singleCluster : result) {
            System.out.println(singleCluster);
        }
        System.out.println("-----------------------------------------------------------------------------------------");
    }
}
