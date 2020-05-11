package query3;

import spire.random.rng.Serial;

import java.io.Serializable;
import java.util.List;

public class CountryDataQuery3 implements Serializable {
    private String name;
    private String month;
    private List<Double> values;
    private Double slope;

    public CountryDataQuery3(String name, String month, List<Double> values) {
        this.name = name;
        this.month = month;
        this.values = values;
        this.slope = null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public List<Double> getValues() {
        return values;
    }

    public void setValues(List<Double> values) {
        this.values = values;
    }

    public Double getSlope() {
        return slope;
    }

    public void setSlope(Double slope) {
        this.slope = slope;
    }
}
