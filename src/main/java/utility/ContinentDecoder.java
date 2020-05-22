package utility;

import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Class used to detect the continent from GeoCoordinate of a country
 */
public class ContinentDecoder {

    /**
     * Those are the information needed to use the first rest api (Nominatim) service we chose to perform the reverse
     * geo-coding, it has been marked as deprecated due to the better performance gained using the BigDataCloud api
     */
    private static final String URL_NOMINATIM = "https://nominatim.openstreetmap.org/reverse?format=json";
    private static final String LATHEAD_NOMINATIM = "&lat=";
    private static final String LONHEAD_NOMINATIM = "&lon=";

    /**
     * Those are the information needed to use the second rest api (BigDataCloud) service we chose to perform
     * the reverse geo-coding
     */
    private static final String URL_BDCLOUD = "https://api.bigdatacloud.net/data/reverse-geocode?";
    private static final String LATHEAD_BDCLOUD = "latitude=";
    private static final String LONHEAD_BDCLOUD = "&longitude=";
    private static final String API_KEY = "&key=98848b588a2f4b3896e280bd5bfae1bc";

    /**
     * Used to perform the reverse geo-coding using Nominatim rest api
     * Slower api service, should move to "detectContinentOnWebBigDataCloud" function
     * @param coordinate GeoCoordinate containing latitude and longitude
     * @return a string representing the continent where the GeoCoordinate lies
     */
    @Deprecated
    private static String detectContinentOnWebNominatim(GeoCoordinate coordinate) {
        String httpUrl = URL_NOMINATIM + LATHEAD_NOMINATIM + coordinate.getLatitude().toString() + LONHEAD_NOMINATIM +
                coordinate.getLongitude().toString();
        try {
            InputStream inputStream = new URL(httpUrl).openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String jsonResponse = readAll(reader);
            JSONObject object = new JSONObject(jsonResponse);
            if (object.has("error")) {
                return "";
            } else {
                String countryCode = object.getJSONObject("address").getString("country_code").toUpperCase();
                return Codes.valueOf(countryCode).getContinent();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Used to perform the reverse geo-coding using BigDataCloud rest api
     * @param coordinate GeoCoordinate containing latitude and longitude
     * @return a string representing the continent where the GeoCoordinate lies
     */
    private static String detectContinentOnWebBigDataCloud(GeoCoordinate coordinate) {
        String httpUrl = URL_BDCLOUD + LATHEAD_BDCLOUD + coordinate.getLatitude().toString() + LONHEAD_BDCLOUD +
                coordinate.getLongitude().toString() + API_KEY;

        try {
            InputStream inputStream = new URL(httpUrl).openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String jsonResponse = readAll(reader);
            JSONObject object = new JSONObject(jsonResponse);
            if (object.has("error")) {
                return "";
            } else {
                String countryCode = object.getString("countryCode").toUpperCase();
                return Codes.valueOf(countryCode).getContinent();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Used to perform the reverse geo-coding using boundaries
     * @param coordinate GeoCoordinate containing latitude and longitude
     * @return a string representing the continent where the GeoCoordinate lies
     */
    private static String detectContinentByBoundaries(GeoCoordinate coordinate) {
        try {
            if (Continents.NORTH_AMERICA_1.contains(coordinate) || Continents.NORTH_AMERICA_2.contains(coordinate) ||
                    Continents.SOUTH_AMERICA.contains(coordinate)) {
                return "America";
            } else if (Continents.AFRICA.contains(coordinate)) {
                return "Africa";
            } else if (Continents.ASIA_1.contains(coordinate) || Continents.ASIA_2.contains(coordinate)) {
                return "Asia";
            } else if (Continents.EUROPE.contains(coordinate)) {
                return "Europe";
            } else if (Continents.OCEANIA.contains(coordinate)) {
                return "Oceania";
            } else if (Continents.ANTARCTICA.contains(coordinate)) {
                return "Antarctica";
            } else {
                return "";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * Generic reverse geo-coding function that first tries to detect the continent using boundaries (coarse grain
     * info) and if this faster method fails it perform a call to a rest api service to detect the continent
     * @param coordinate GeoCoordinate containing latitude and longitude
     * @return a string representing the continent where the GeoCoordinate lies
     */
    public static String detectContinent(GeoCoordinate coordinate) {
        String continent = detectContinentByBoundaries(coordinate);
        if (continent.equals("")) {
            continent = detectContinentOnWebBigDataCloud(coordinate);
        }
        return continent;
    }

    /**
     * Function to convert to string the json taken from the reader
     * @param reader
     * @return a string containing the whole content retrieved from the reader
     * @throws IOException
     */
    private static String readAll(Reader reader) throws IOException {
        StringBuilder builder = new StringBuilder();
        int read;
        while ((read = reader.read()) != -1) {
            builder.append((char)read);
        }
        return builder.toString();
    }
}