package com.maxdemarzi;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.maxdemarzi.results.MapResult;
import com.maxdemarzi.results.StringResult;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.maxdemarzi.Utilities.getMaxDistance;

public class Flights {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    private static GraphDatabaseService graph;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    static final Integer DEFAULT_RECORD_LIMIT = 50;
    static final Integer DEFAULT_TIME_LIMIT = 2000; // 2 Seconds
    private static final Integer DEFAULT_PATH_LIMIT = 100;
    private static final PathExpander fliesToExpander = PathExpanders.forTypeAndDirection(RelationshipTypes.FLIES_TO, Direction.OUTGOING);
    private static final FlightComparator FLIGHT_COMPARATOR = new FlightComparator();

    private static final BidirectionalFliesToExpander bidirectionalFliesToExpander = new BidirectionalFliesToExpander();
    private static final InitialBranchState.State<Double> ibs = new InitialBranchState.State<>(0.0, 0.0);

    private static final LoadingCache<String, ArrayList<HashMap<RelationshipType, Set<RelationshipType>>>> allowedCache = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(15, TimeUnit.MINUTES)
            .refreshAfterWrite(15, TimeUnit.MINUTES)
            .build(key -> allowedRels(key));

    private static ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> allowedRels(String key) {
        // calculate valid Relationships
        Node departureAirport = graph.findNode(Labels.Airport, "code", key.substring(0,3));
        Node arrivalAirport = graph.findNode(Labels.Airport, "code", key.substring(4,7));
        Double maxDistance = getMaxDistance(departureAirport, arrivalAirport);
        return getValidPaths(departureAirport, arrivalAirport, maxDistance);
    }

    static LoadingCache<String, Set<String>> nearCache = Caffeine.newBuilder()
            .maximumSize(15_000)
            .expireAfterWrite(1, TimeUnit.DAYS)
            .refreshAfterWrite(1, TimeUnit.DAYS)
            .build(key -> nearbyAirports(key));

    private static Set<String> nearbyAirports(String key) {
        // calculate valid Relationships
        Node airport = graph.findNode(Labels.Airport, "code", key.substring(0,3));
        String[] near = (String[])airport.getProperty("near", new String[0]);
        return new HashSet<>(Arrays.asList(near));
    }

    @Description("com.maxdemarzi.clear_flight_cache() | Clear cached flight data")
    @Procedure(name = "com.maxdemarzi.clear_flight_cache", mode = Mode.SCHEMA)
    public Stream<StringResult> clearCache() {
        allowedCache.invalidateAll();
        nearCache.invalidateAll();
        return Stream.of(new StringResult("Cache Cleared"));
    }

    @Description("com.maxdemarzi.flights() | Find Routes between Airports")
    @Procedure(name = "com.maxdemarzi.flights", mode = Mode.SCHEMA)
    public Stream<MapResult> simpleFlightSearch(@Name("from") List<String> from,
                                                @Name("to") List<String> to,
                                                @Name("day") String day) {
        return flightSearch(from, to, day, DEFAULT_RECORD_LIMIT, DEFAULT_TIME_LIMIT);
    }

    @Description("com.maxdemarzi.flightSearch() | Find Routes between Airports")
    @Procedure(name = "com.maxdemarzi.flightSearch", mode = Mode.SCHEMA)
    public Stream<MapResult> flightSearch(@Name("from") List<String> from,
                                          @Name("to") List<String> to,
                                          @Name("day") String day,
                                          @Name("recordLimit") Number recordLimit,
                                          @Name("timeLimit") Number timeLimit) {
        graph = db;
        ArrayList<MapResult> results = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            for (String fromKey : getAirportDayKeys(from, day)) {
                Node departureAirport = db.findNode(Labels.Airport, "code", fromKey.substring(0,3));
                Node departureAirportDay = db.findNode(Labels.AirportDay, "key", fromKey);

                if (!(departureAirportDay == null)) {
                    for (String toKey : getAirportDayKeys(to, (String) day)) {
                        Node arrivalAirport = db.findNode(Labels.Airport, "code", toKey.substring(0,3));
                        Double maxDistance = getMaxDistance(departureAirport, arrivalAirport);

                        // Get Valid Traversals from Each Departure Airport at each step along the valid paths
                        ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> validRels = allowedCache.get(fromKey.substring(0,3) + "-" + toKey.substring(0,3));

                        // If we found valid paths from departure airport to destination airport
                        if ( !validRels.isEmpty()) {
                            // Prepare and run the second traversal
                            PathRestrictedExpander pathRestrictedExpander = new PathRestrictedExpander(toKey.substring(0, 3), timeLimit.intValue(), validRels);
                            RouteCostEvaluator routeCostEvaluator = new RouteCostEvaluator();
                            PathFinder<WeightedPath> dijkstra = GraphAlgoFactory.dijkstra(pathRestrictedExpander, routeCostEvaluator, recordLimit.intValue());
                            secondTraversal(results, recordLimit.intValue(), departureAirportDay, arrivalAirport, maxDistance, dijkstra);
                        } else {
                            System.out.println("no valid paths found");
                        }
                    }
                }
            }
            tx.success();
        }
        Collections.sort(results, FLIGHT_COMPARATOR);
        return results.stream();
    }

    private static ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> getValidPaths(Node departureAirport, Node arrivalAirport, Double maxDistance) {
        ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> validRels = new ArrayList<>();

        TraversalDescription td = graph.traversalDescription()
                .breadthFirst()
                .expand(bidirectionalFliesToExpander, ibs)
                .uniqueness(Uniqueness.NODE_PATH)
                .evaluator(Evaluators.toDepth(2));

        BidirectionalTraversalDescription bidirtd = graph.bidirectionalTraversalDescription()
                .mirroredSides(td)
                .collisionEvaluator(new CollisionEvaluator());

        for (org.neo4j.graphdb.Path route : bidirtd.traverse(departureAirport, arrivalAirport)) {
            Double distance = 0D;
            for (Relationship relationship : route.relationships()) {
                distance += (Double) relationship.getProperty("distance", 25000D);
            }

            if (distance < maxDistance){
                String code = null;
                RelationshipType relationshipType = null;
                int count = 0;
                for (Node node : route.nodes()) {
                    if (relationshipType == null) {
                        code = (String)node.getProperty("code");
                        relationshipType = RelationshipType.withName(code + "_FLIGHT");
                    } else {
                        HashMap<RelationshipType, Set<RelationshipType>> validAt;
                        if (validRels.size() <= count) {
                            validAt = new HashMap<>();
                        } else {
                            validAt = validRels.get(count);
                        }
                        Set<RelationshipType> valid = validAt.getOrDefault(relationshipType, new HashSet<>());
                        String newcode = (String)node.getProperty("code");
                        if(!nearCache.get(code).contains(newcode)) {
                            RelationshipType newRelationshipType = RelationshipType.withName(newcode + "_FLIGHT");
                            valid.add(newRelationshipType);
                            validAt.put(relationshipType, valid);
                            if (validRels.size() <= count) {
                                validRels.add(count, validAt);
                            } else {
                                validRels.set(count, validAt);
                            }
                            relationshipType = newRelationshipType;
                            code = newcode;
                            count++;
                        }
                    }
                }
            }
        } return validRels;
    }

    private void secondTraversal(ArrayList<MapResult> results, Integer recordLimit, Node departureAirportDay, Node arrivalAirport, Double maxDistance, PathFinder<WeightedPath> dijkstra) {
        for (org.neo4j.graphdb.Path position : dijkstra.findAllPaths(departureAirportDay, arrivalAirport)) {
            if(results.size() < recordLimit) {
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
    }

    private ArrayList<String> getAirportDayKeys(List<String> from, String day) {
        ArrayList<String> departureAirportDayKeys = new ArrayList<>();
        for(String code : from) {
            String key = code + "-" + day;
            departureAirportDayKeys.add(key);
        }
        return departureAirportDayKeys;
    }
}
