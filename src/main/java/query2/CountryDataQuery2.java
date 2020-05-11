package query2;

import utility.QueryUtility;
import utility.GeoCoordinate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class CountryDataQuery2 implements Serializable {
    private GeoCoordinate coordinate;
    private List<Double> covidConfirmedCases;
    private String name;

    public CountryDataQuery2(GeoCoordinate geoCoordinate, List<String> covidCases, String name) {
        this.coordinate = geoCoordinate;
        this.covidConfirmedCases = QueryUtility.toPunctualData(covidCases);
        this.name = name;
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
            CountryDataQuery2 countryData = new CountryDataQuery2(null, Arrays.asList(split).subList(4, split.length), null);
            System.out.println(countryData.covidConfirmedCases);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
