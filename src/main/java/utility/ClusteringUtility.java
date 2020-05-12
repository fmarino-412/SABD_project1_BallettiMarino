package utility;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import query3.CountryDataQuery3;
import scala.Tuple2;

import java.util.List;

public class ClusteringUtility {

    private static final boolean NAIVE = Boolean.FALSE;
    private static final Integer CLUSTERS = 4;
    private static final Integer ITERATION = 20;

    public void clusteringNaive() {

    }

    public static void clusteringMLlib(JavaPairRDD<String, List<Tuple2<Double, CountryDataQuery3>>> data) {

        /*JavaRDD<Vector> parsedData = data.map(
                tuple -> {
                    double[] values = new double[tuple._2().size()];
                    for (int i = 0; i < tuple._2().size(); i++) {
                        values[i] = tuple._2().get(i)._1();
                    }
                    return Vectors.dense(values);
                }
        );*/

        data.map(
                tuple -> {
                    double[] values = new double[tuple._2().size()];
                    for (int i = 0; i < tuple._2().size(); i++) {
                        values[i] = tuple._2().get(i)._1();
                    }
                    Vector v = Vectors.dense(values);
                    KMeansModel model = KMeans.train(v, CLUSTERS, ITERATION);
                }
        )

        KMeansModel model = KMeans.train(parsedData.rdd(), CLUSTERS, ITERATION);

        System.out.println("Cluster centers:");
        for (Vector center: model.clusterCenters()) {
            System.out.println(" " + center);
        }
    }
}
