package query2;

import utility.GeoCoordinate;
import utility.QueryUtility;

import java.io.Serializable;
import java.util.List;

public class CountryDataQuery2 implements Serializable {
    private final GeoCoordinate coordinate;
    private final List<Double> covidConfirmedCases;

    public CountryDataQuery2(GeoCoordinate geoCoordinate, List<String> covidCases) {
        this.coordinate = geoCoordinate;
        this.covidConfirmedCases = QueryUtility.toPunctualData(covidCases);
    }

    public GeoCoordinate getCoordinate() {
        return coordinate;
    }

    public List<Double> getCovidConfirmedCases() {
        return covidConfirmedCases;
    }

}
