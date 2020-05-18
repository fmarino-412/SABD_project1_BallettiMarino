package utility;

import java.io.Serializable;

/**
 * Class representing the location of a country by latitude and longitude
 */
public class GeoCoordinate implements Serializable {

    private final Double latitude;
    private final Double longitude;

    public GeoCoordinate(Double latitude, Double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public GeoCoordinate(String latitude, String longitude) {
        this.latitude = Double.valueOf(latitude);
        this.longitude = Double.valueOf(longitude);
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

}