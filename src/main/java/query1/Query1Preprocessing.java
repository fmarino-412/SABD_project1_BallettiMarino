package query1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import utility.QueryUtility;

import java.text.SimpleDateFormat;
import java.util.*;

public class Query1Preprocessing {

    public static JavaPairRDD<String, Tuple2<Integer, Integer>> preprocessData(JavaRDD<String> dataset1) {
        return dataset1.mapToPair(
                line -> {
                    // split csv line basing on the separator
                    String[] lineSplit = line.split(",");
                    // create the inner tuple as <cured, swabs>
                    Tuple2<Integer, Integer> innerTuple = new Tuple2<>(Integer.valueOf(lineSplit[1]),
                            Integer.valueOf(lineSplit[2]));
                    // split the date basing on the T character and save just the initial part (year, month, day)
                    String generalDate = (lineSplit[0].split("T"))[0];
                    // create the final tuple
                    return new Tuple2<>(new SimpleDateFormat("yyyy-MM-dd").parse(generalDate), innerTuple);
                }
        ).flatMapToPair(
                tuple -> {
                    List<Tuple2<String, Tuple2<Integer, Integer>>> result = new ArrayList<>();
                    Calendar calendar = new GregorianCalendar(Locale.ITALIAN);
                    calendar.setTime(tuple._1());
                    String key = QueryUtility.getFirstDayOfTheWeek(calendar.get(Calendar.WEEK_OF_YEAR),
                            calendar.get(Calendar.YEAR));
                    result.add(new Tuple2<>(key, tuple._2()));
                    if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
                        calendar.add(Calendar.WEEK_OF_YEAR, 1);
                        String key2 = QueryUtility.getFirstDayOfTheWeek(calendar.get(Calendar.WEEK_OF_YEAR),
                                calendar.get(Calendar.YEAR));
                        result.add(new Tuple2<>(key2, tuple._2()));
                    }
                    return result.iterator();
                }
        );
    }
}
