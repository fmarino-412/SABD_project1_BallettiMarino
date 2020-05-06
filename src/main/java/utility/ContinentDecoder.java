package utility;

import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ContinentDecoder {

    private static final String URL = "https://nominatim.openstreetmap.org/reverse?format=json";
    private static final String LATHEAD = "&lat=";
    private static final String LONHEAD = "&lon=";

    private static String readAll(Reader reader) throws IOException {
        StringBuilder builder = new StringBuilder();
        int read;
        while ((read = reader.read()) != -1) {
            builder.append((char)read);
        }
        return builder.toString();
    }

    public static String detectContinent(GeoCoordinate coordinate) {
        String httpUrl = URL + LATHEAD + coordinate.getLatitude().toString() + LONHEAD +
                coordinate.getLongitude().toString();
        try {
            InputStream inputStream = new URL(httpUrl).openStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String jsonResponse = readAll(reader);
            JSONObject object = new JSONObject(jsonResponse);
            if (object.has("error")) {
                return null;
            } else {
                String countryCode = object.getJSONObject("address").getString("country_code").toUpperCase();
                return Codes.valueOf(countryCode).getContinent();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}