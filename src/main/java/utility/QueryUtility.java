package utility;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

public class QueryUtility {

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

    public static Calendar getDataset1StartDate() {
        return new GregorianCalendar(2020, Calendar.FEBRUARY, 24);
    }

    public static Calendar getDataset2StartDate() {
        return new GregorianCalendar(2020, Calendar.JANUARY, 22);
    }
}
