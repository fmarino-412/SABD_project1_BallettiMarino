package utility;

import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Class that provides basic function to help queries' workflow
 */
public class QueryUtility {

    /**
     * Scope: Query 2 and Query 3
     * Used to convert cumulative data in string format to punctual data
     * @param cumulativeData list of string representing cumulative data
     * @return list of double representing the punctual data related to cumulativeData param
     */
    public static List<Double> toPunctualData(List<String> cumulativeData) {
        double tmp = 0, curr;
        List<Double> conversionResult = new ArrayList<>();
        for (String value : cumulativeData) {
            curr = Double.parseDouble(value);

            // there is an error in the dataset so we assume no increment in confirmed cases in such days
            if (curr - tmp < 0) {
                conversionResult.add(0.0);
            }
            // normal behaviour
            else {
                conversionResult.add(curr - tmp);
                tmp = curr;
            }
        }
        return conversionResult;
    }

    /**
     * Scope: Query 1
     * Used to convert weekly cumulative cured and swabs tuple to punctual values
     * @param tuple containing cumulative data to convert
     * @return a tuple composed of two array lists, one for punctual cured values and the other for punctual swabs
     * values
     */
    public static Tuple2<ArrayList<Integer>, ArrayList<Integer>> toPunctualData(
            Tuple2<String, Iterable<Tuple2<Integer, Integer>>> tuple) {

        ArrayList<Integer> cured = new ArrayList<>();
        ArrayList<Integer> swabs = new ArrayList<>();

        int elements = 0;

        // save values of the current week to local structures for analysis
        for (Tuple2<Integer, Integer> element : tuple._2()) {
            cured.add(element._1());
            swabs.add(element._2());
            elements++;
        }

        // order temporally (works since is being applied on cumulative data)
        // precedent week data will be then placed at index 0
        cured.sort(Comparator.naturalOrder());
        swabs.sort(Comparator.naturalOrder());

        String firstWeekKey = QueryUtility.getFirstDayOfTheWeek(
                QueryUtility.getDataset1StartDate().get(Calendar.WEEK_OF_YEAR),
                QueryUtility.getDataset1StartDate().get(Calendar.YEAR));

        // in first week data there are no values from previous week
        if (!firstWeekKey.equals(tuple._1())) {
            if (elements == 1) {
                // week created with only a precedent week element,
                // remove the corresponding RDD element
                return null;
            }

            // not in the first week, there is precedent week data
            // make week data independent from precedent weeks
            for (int i = 1; i < elements; i++) {
                cured.set(i, cured.get(i) - cured.get(0));
                swabs.set(i, swabs.get(i) - swabs.get(0));
            }
            cured.remove(0);
            swabs.remove(0);
            elements = elements - 1;
        }

        // turn from cumulative to punctual data
        for (int i = elements - 1; i > 0; i--) {
            cured.set(i, cured.get(i) - cured.get(i-1));
            swabs.set(i, swabs.get(i) - swabs.get(i -1));
        }

        // return [[List of punctual cured], [List of punctual swabs]]
        return new Tuple2<>(cured, swabs);
    }

    /**
     * Used to get the start date of the first dataset which is statically the 24th of February 2020
     * @return the start date of the first dataset in GregorianCalendar format
     */
    public static Calendar getDataset1StartDate() {
        return new GregorianCalendar(2020, Calendar.FEBRUARY, 24);
    }

    /**
     * Used to get the start date of the second dataset which is statically the 22th of February 2020
     * @return the start date of the second dataset in GregorianCalendar format
     */
    public static Calendar getDataset2StartDate() {
        return new GregorianCalendar(2020, Calendar.JANUARY, 22);
    }

    /**
     * Used to get the date corresponding to the monday of a week in a year
     * @param week integer representing the week number in the year
     * @param year integer representing the actual year
     * @return a string in the format yyyy-MM-dd
     */
    public static String getFirstDayOfTheWeek(int week, int year) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.WEEK_OF_YEAR, week);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime());
    }
}
