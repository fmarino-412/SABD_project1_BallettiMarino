package query2;

import utility.GeoCoordinate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CountryData implements Serializable {
    private GeoCoordinate coordinate;
    private List<Double> covidConfirmedCases;
    private String name;

    public CountryData(GeoCoordinate geoCoordinate, List<String> covidCases, String name) {
        this.coordinate = geoCoordinate;
        this.covidConfirmedCases = new ArrayList<>();
        this.name = name;
        double tmp = 0;
        for (String covidCase : covidCases) {
            this.covidConfirmedCases.add(Double.parseDouble(covidCase) - tmp);
            tmp = Double.parseDouble(covidCase);
        }
    }

    public GeoCoordinate getCoordinate() {
        return coordinate;
    }

    public List<Double> getCovidConfirmedCases() {
        return covidConfirmedCases;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static void test() {
        try {
            BufferedReader br = Files.newBufferedReader(Paths.get("data/DS2.csv"));
            String csvLine = br.readLine();
            String[] split = csvLine.split(",");
            CountryData countryData = new CountryData(null, Arrays.asList(split).subList(4, split.length), null);
            System.out.println(countryData.covidConfirmedCases);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
