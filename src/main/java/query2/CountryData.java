package query2;

import utility.GeoCoordinate;

import java.util.ArrayList;
import java.util.List;

public class CountryData {
    private GeoCoordinate coordinate;
    private Double covidTrendlineCoefficient;
    private List<Double> covidConfirmedCases;
    private String name;

    public CountryData(GeoCoordinate geoCoordinate, List<String> covidCases, String s) {
        this.coordinate = geoCoordinate;
        this.covidConfirmedCases = new ArrayList<>();
        this.name = s;
        for (String covidCase : covidCases) {
            this.covidConfirmedCases.add(Double.valueOf(covidCase));
        }
    }

    public CountryData(CountryData country, Double coefficient) {
        this.coordinate = country.getCoordinate();
        this.covidConfirmedCases = country.getCovidConfirmedCases();
        this.name = country.getName();
        this.covidTrendlineCoefficient = coefficient;
    }

    public CountryData(CountryData country, String s) {
        this.coordinate = country.getCoordinate();
        this.covidConfirmedCases = country.getCovidConfirmedCases();
        this.covidTrendlineCoefficient = country.getCovidTrendlineCoefficient();
        this.name = s;
    }

    public GeoCoordinate getCoordinate() {
        return coordinate;
    }

    public List<Double> getCovidConfirmedCases() {
        return covidConfirmedCases;
    }

    public Double getCovidTrendlineCoefficient() {
        return covidTrendlineCoefficient;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
