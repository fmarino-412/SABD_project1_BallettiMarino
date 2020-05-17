package utility;

import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ContinentDecoder {

    private static final String URL_NOMINATIM = "https://nominatim.openstreetmap.org/reverse?format=json";
    private static final String LATHEAD_NOMINATIM = "&lat=";
    private static final String LONHEAD_NOMINATIM = "&lon=";

    private static final String URL_BDCLOUD = "https://api.bigdatacloud.net/data/reverse-geocode-client?";
    private static final String LATHEAD_BDCLOUD = "latitude=";
    private static final String LONHEAD_BDCLOUD = "&longitude=";

    private static String readAll(Reader reader) throws IOException {
        StringBuilder builder = new StringBuilder();
        int read;
        while ((read = reader.read()) != -1) {
            builder.append((char)read);
        }
        return builder.toString();
    }

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

    private static String detectContinentOnWebBigDataCloud(GeoCoordinate coordinate) {
        String httpUrl = URL_BDCLOUD + LATHEAD_BDCLOUD + coordinate.getLatitude().toString() + LONHEAD_BDCLOUD +
                coordinate.getLongitude().toString();

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

    public static String detectContinent(GeoCoordinate coordinate) {
        String continent = detectContinentByBoundaries(coordinate);
        if (continent.equals("")) {
            continent = detectContinentOnWebBigDataCloud(coordinate);
        }
        return continent;
    }

    public static void testLocal() {
        try {
            BufferedReader br = Files.newBufferedReader(Paths.get("data/DS2.csv"));
            String csvLine = br.readLine();
            int i = 1;
            String continent;
            while (csvLine != null) {
                String[] split = csvLine.split(",");
                continent = detectContinentByBoundaries(new GeoCoordinate(Double.parseDouble(split[2]),
                        Double.parseDouble(split[3])));
                if (continent.equals("")) {
                    System.err.printf("%d)\tError retrieving: %s\n", i, split[1]);
                } else {
                    System.out.printf("%d)\t%s:\t%s\n", i, split[1], continent);
                }
                i++;
                csvLine = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testGlobal() {
        try {
            BufferedReader br = Files.newBufferedReader(Paths.get("data/DS2.csv"));
            String csvLine = br.readLine();
            int i = 1;
            String continent;
            while (csvLine != null) {
                String[] split = csvLine.split(",");
                continent = detectContinent(new GeoCoordinate(Double.parseDouble(split[2]),
                        Double.parseDouble(split[3])));
                if (continent.equals("")) {
                    System.err.printf("%d)\tError retrieving: %s\n", i, split[1]);
                } else {
                    System.out.printf("%d)\t%s:\t%s\n", i, split[1], continent);
                }
                i++;
                csvLine = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}