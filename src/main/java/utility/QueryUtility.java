package utility;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * Class that provides basic function to help queries' workflow
 */
public class QueryUtility {

    /**
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
