package com.maxdemarzi;

import com.maxdemarzi.results.MapResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * Sort by Score
 * Sort by Departure
 * Sort by Distance
 * Sort by First Flight Code
 */
class FlightComparator<T extends Comparable<T>> implements Comparator<MapResult> {
    @Override
    public int compare(MapResult flights1, MapResult flights2) {
        int c;
        c = ((Integer)flights1.value.get("score")).compareTo((Integer)flights2.value.get("score"));

        if (c == 0) {
            HashMap firstFlight1 = (HashMap)((ArrayList)flights1.value.get("flights")).get(0);
            HashMap firstFlight2 = (HashMap)((ArrayList)flights2.value.get("flights")).get(0);

            c = ((String)firstFlight1.getOrDefault("departs_at", "")).compareTo((String)(firstFlight2.getOrDefault("departs_at", "")));

            if (c == 0) {
                c = ((Integer)flights1.value.get("distance")).compareTo((Integer) flights2.value.get("distance"));
                if (c == 0) {
                    c = ((String)firstFlight1.get("code")).compareTo((String)(firstFlight2.get("code")));
                }
            }
        }

        return c;
    }
}