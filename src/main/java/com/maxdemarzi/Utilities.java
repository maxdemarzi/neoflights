package com.maxdemarzi;

import org.neo4j.graphdb.Node;

class Utilities {


    static Double getMaxDistance(Node departureAirport, Node arrivalAirport) {
        Double minDistance = GetMileage((Double)departureAirport.getProperty("latitude"),
                (Double)departureAirport.getProperty("longitude"),
                (Double)arrivalAirport.getProperty("latitude"),
                (Double)arrivalAirport.getProperty("longitude"));
        return 500 + (2 * minDistance);
    }

    private static Double GetMileage(double lat1, double lon1, double lat2, double lon2)
    {
        // ***********************
        //  calculate the mileage
        // ***********************
        //
        // The formula for distance in NM given in the
        // Aviation Formulary v1.37
        // (http://williams.best.vwh.net/avform.htm) is:
        //  d = acos(sin(lat1)*sin(lat2)+cos(lat1)*cos(lat2)*cos(lon1-lon2))

        Double x = (Math.sin(lat1) * Math.sin(lat2)) + (Math.cos(lat1) * Math.cos(lat2) * Math.cos(lon1 - lon2));

        // calculate distance based on approx. diameter of earth, round up and return the mileage
        return (Math.acos(x) * 3959.0) + 0.5;

    }
}
