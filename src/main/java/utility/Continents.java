package utility;

/**
 * Class representing the six continents boundary (coarse grain)
 */
public class Continents {

    /**
     * North America boundaries' point
     */
    private static final Double[] LAT_N_AM = {90.0, 90.0, 78.13, 57.5, 15.0, 15.0, 1.25, 1.25, 51.0, 60.0, 60.0};
    private static final Double[] LON_N_AM = {-168.75, -10.0, -10.0, -37.5, -30.0, -75.0, -82.5, -105.0, -180.0, -180.0,
            -168.75};
    private static final Double[] LAT_N_AM_2 = {51.0, 51.0, 60.0};
    private static final Double[] LON_N_AM_2 = {166.6, 180.0, 180.0};

    /**
     * South America boundaries' point
     */
    private static final Double[] LAT_S_AM = {1.25, 1.25, 15.0, 15.0, -60.0, -60.0};
    private static final Double[] LON_S_AM = {-105.0, -82.5, -75.0, -30.0, -30.0, -105.0};

    /**
     * Asia boundaries' point
     */
    private static final Double[] LAT_ASI = {90.0, 42.5, 42.5, 40.79, 41.0, 40.55, 40.4, 40.05, 39.17, 35.46, 33.0,
            31.74, 29.54, 27.78, 11.3, 12.5, -60.0, -60.0, -31.88, -11.88, -10.27, 33.13, 51.0, 60.0, 90.0};
    private static final Double[] LON_ASI = {77.5, 48.8, 30.0, 28.81, 29.0, 27.31, 26.75, 26.36, 25.19, 27.91, 27.5,
            34.58, 34.92, 34.46, 44.3, 52.0, 75.0, 110.0, 110.0, 110.0, 140.0, 140.0, 166.6, 180.0, 180.0};
    private static final Double[] LAT_ASI_2 = {90.0, 90.0, 60.0, 60.0};
    private static final Double[] LON_ASI_2 = {-180.0, -168.75, -168.75, -180.0};

    /**
     * Africa boundaries' point
     */
    private static final Double[] LAT_AFR = {15.0, 28.25, 35.42, 38.0, 33.0, 31.74, 29.54, 27.78, 11.3, 12.5, -60.0, -60.0};
    private static final Double[] LON_AFR = {-30.0, -13.0, -10.0, 10.0, 27.5, 34.58, 34.92, 34.46, 44.3, 52.0, 75.0, -30.0};

    /**
     * Europe boundaries' point
     */
    private static final Double[] LAT_EUR = {90.0, 90.0, 42.5, 42.5, 40.79, 41.0, 40.55, 40.40, 40.05, 39.17, 35.46, 33.0,
            38.0, 35.42, 28.25, 15.0, 57.5, 78.13};
    private static final Double[] LON_EUR = {-10.0, 77.5, 48.8, 30.0, 28.81, 29.0, 27.31, 26.75, 26.36, 25.19, 27.91, 27.5,
            10.0, -10.0, -13.0, -30.0, -37.5, -10.0};

    /**
     * Oceania boundaries' point
     */
    private static final Double[] LAT_OCE = {-11.88, -10.27, -10.0, -30.0, -52.5, -31.88};
    private static final Double[] LON_OCE = {110.0, 140.0, 145.0, 161.25, 142.5, 110.0};

    /**
     * Antarctica boundaries' point
     */
    private static final Double[] LAT_ANT = {-60.0, -60.0, -90.0, -90.0};
    private static final Double[] LON_ANT = {-180.0, 180.0, 180.0, -180.0};


    public static Boundary NORTH_AMERICA_1 = new Boundary(LAT_N_AM, LON_N_AM);
    public static Boundary NORTH_AMERICA_2 = new Boundary(LAT_N_AM_2, LON_N_AM_2);
    public static Boundary SOUTH_AMERICA = new Boundary(LAT_S_AM, LON_S_AM);
    public static Boundary ASIA_1 = new Boundary(LAT_ASI, LON_ASI);
    public static Boundary ASIA_2 = new Boundary(LAT_ASI_2, LON_ASI_2);
    public static Boundary AFRICA = new Boundary(LAT_AFR, LON_AFR);
    public static Boundary EUROPE = new Boundary(LAT_EUR, LON_EUR);
    public static Boundary OCEANIA = new Boundary(LAT_OCE, LON_OCE);
    public static Boundary ANTARCTICA = new Boundary(LAT_ANT, LON_ANT);
}
