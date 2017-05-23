package com.maxdemarzi;

import com.maxdemarzi.results.MapResult;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.stream.Stream;

import static com.maxdemarzi.Utilities.getMaxDistance;

public class Flights {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    static final Integer DEFAULT_RECORD_LIMIT = 50;
    static final Integer DEFAULT_TIME_LIMIT = 2000; // 2 Seconds
    private static final Integer DEFAULT_PATH_LIMIT = 100;
    private static final PathExpander fliesToExpander = PathExpanders.forTypeAndDirection(RelationshipTypes.FLIES_TO, Direction.OUTGOING);
    private static final FlightComparator FLIGHT_COMPARATOR = new FlightComparator();

    @Description("com.maxdemarzi.flights() | Find Routes between 2 Airports")
    @Procedure(name = "com.maxdemarzi.flights", mode = Mode.SCHEMA)
    public Stream<MapResult> simpleFlightSearch(@Name("from") String from,
                                                @Name("to") String to,
                                                @Name("day") String day) {
        return flightSearch(from, to, day, DEFAULT_RECORD_LIMIT, DEFAULT_TIME_LIMIT);
    }

    @Description("com.maxdemarzi.flightSearch() | Find Routes between 2 Airports")
    @Procedure(name = "com.maxdemarzi.flightSearch", mode = Mode.SCHEMA)
    public Stream<MapResult> flightSearch(@Name("from") String from,
                                          @Name("to") String to,
                                          @Name("day") String day,
                                          @Name("recordLimit") Number recordLimit,
                                          @Name("timeLimit") Number timeLimit) {
        ArrayList<MapResult> results = new ArrayList<>();

        // Get departure and arrival airports
        Node departureAirport = db.findNode(Labels.Airport, "code", from);
        Node departureAirportDay = db.findNode(Labels.AirportDay, "key", from + "-" + day);
        Node arrivalAirport = db.findNode(Labels.Airport, "code", to);
        Double minDistance = 25000D;
        Double maxDistance = getMaxDistance(departureAirport, arrivalAirport);

        // First traversal finds potential airport to airport routes
        Set<RelationshipType> validPaths = new HashSet<>();
        PathFinder<WeightedPath> airportDijkstra = GraphAlgoFactory.dijkstra(fliesToExpander, "distance", DEFAULT_PATH_LIMIT);
        for (org.neo4j.graphdb.Path route : airportDijkstra.findAllPaths(departureAirport, arrivalAirport)) {
            Double distance = 0D;
            Set <RelationshipType> validPath = new HashSet<>();
            for (Node node : route.nodes()) {
                validPath.add(RelationshipType.withName(node.getProperty("code") + "_FLIGHT"));
            }
            for (Relationship relationship : route.relationships()) {
                distance += (Double) relationship.getProperty("distance", 0D);
            }

            if (distance < minDistance) {
                minDistance = distance;
            }

            if (distance < (500 + (2 * minDistance))){
                validPaths.addAll(validPath);
            }
        }

        // Second traversal find actual flight routes based on potential airport routes
        PathRestrictedExpander pathRestrictedExpander =
                new PathRestrictedExpander(to, timeLimit.intValue(),
                        validPaths.toArray(new RelationshipType[validPaths.size()]));

        RouteCostEvaluator routeCostEvaluator = new RouteCostEvaluator();
        PathFinder<WeightedPath> dijkstra = GraphAlgoFactory.dijkstra(pathRestrictedExpander, routeCostEvaluator, recordLimit.intValue());

        for (org.neo4j.graphdb.Path position : dijkstra.findAllPaths(departureAirportDay, arrivalAirport)) {
            if(results.size() < recordLimit.intValue()) {
                HashMap<String, Object> result = new HashMap<>();
                ArrayList<Map> flights = new ArrayList<>();
                Double distance = 0D;
                ArrayList<Node> nodes =  new ArrayList<>();
                for (Node node : position.nodes()) {
                    nodes.add(node);
                    distance += ((Number) node.getProperty("distance", 0)).doubleValue();
                }
                if (distance < maxDistance) {

                    for (int i = 0; i < nodes.size() - 1; i++) {
                        if (i % 2 == 1) {
                            Map flightInfo = nodes.get(i).getAllProperties();
                            flightInfo.put("origin", ((String)nodes.get(i-1).getProperty("key")).substring(0,3));
                            flightInfo.put("destination", ((String)nodes.get(i+1).getProperty("key")).substring(0,3));
                            flightInfo.remove("departs");
                            flightInfo.remove("arrives");
                            flights.add(flightInfo);
                        }
                    }

                    result.put("flights", flights);
                    result.put("score", position.length() - 2);
                    result.put("distance", distance.intValue());
                    results.add(new MapResult(result));
                }
            }
        }
        Collections.sort(results, FLIGHT_COMPARATOR);
        return results.stream();
    }


}
