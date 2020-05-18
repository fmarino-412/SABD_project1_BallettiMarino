package query3;

import java.io.Serializable;
import java.util.List;

public class CountryDataQuery3 implements Serializable {
    private final String name;
    private final List<Double> values;
    private Double slope;

    public CountryDataQuery3(String name, List<Double> values) {
        this.name = name;
        this.values = values;
        this.slope = null;
    }

    public String getName() {
        return name;
    }

    public List<Double> getValues() {
        return values;
    }

    public Double getSlope() {
        return slope;
    }

    public void setSlope(Double slope) {
        this.slope = slope;
    }
}
