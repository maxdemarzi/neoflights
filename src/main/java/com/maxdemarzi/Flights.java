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
import org.neo4j.helpers.collection.Pair;
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

    // We will return a maximum of 50 records unless told otherwise in our input
    private static final Integer DEFAULT_RECORD_LIMIT = 50;

    // The query will stop and return whatever results we have at the 2 second mark
    private static final Integer DEFAULT_TIME_LIMIT = 2000; // 2 Seconds

    private static final BidirectionalFliesToExpander bidirectionalFliesToExpander = new BidirectionalFliesToExpander();
    private static final InitialBranchState.State<Double> ibs = new InitialBranchState.State<>(0.0, 0.0);

    // Sort results by Score, Departure time, Distance and lastly the first flight code
    private static final FlightComparator FLIGHT_COMPARATOR = new FlightComparator();

    // Since Airports rarely stop having flights between each other, we will cache our first traversal.
    // A cache hit can save us around 40-80ms of query time
    private static final LoadingCache<String, ArrayList<HashMap<RelationshipType, Set<RelationshipType>>>> allowedCache = Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .refreshAfterWrite(1, TimeUnit.HOURS)
            .build(Flights::allowedRels);

    private static ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> allowedRels(String key) {
        // calculate valid Relationships
        Node departureAirport = graph.findNode(Labels.Airport, "code", key.substring(0,3));
        Node arrivalAirport = graph.findNode(Labels.Airport, "code", key.substring(4,7));
        Double maxDistance = getMaxDistance(departureAirport, arrivalAirport);
        return getValidPaths(departureAirport, arrivalAirport, maxDistance);
    }

    // As a further optimization we could cache all node property reads
    // Leaving out for now, but you can create a general one like this or
    // create multiple caches one for each property key you want to cache
    private static final LoadingCache<Pair<Long, String>, Object> propertyCache = Caffeine.newBuilder()
            .maximumSize(1_000_000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .refreshAfterWrite(1, TimeUnit.HOURS)
            .build(Flights::getNodeProperty);

    private static Object getNodeProperty(Pair<Long, String> key) {
        return graph.getNodeById(key.first()).getProperty(key.other(), null);
    }


    // For testing purposes and for major changes to the underlying data, clear the cache.
    @Description("com.maxdemarzi.clear_flight_cache() | Clear cached flight data")
    @Procedure(name = "com.maxdemarzi.clear_flight_cache", mode = Mode.SCHEMA)
    public Stream<StringResult> clearCache() {
        allowedCache.invalidateAll();
        propertyCache.invalidateAll();
        return Stream.of(new StringResult("Cache Cleared"));
    }

    // Simpler flight search procedure with sensible defaults
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
                    for (String toKey : getAirportDayKeys(to, day)) {
                        Node arrivalAirport = db.findNode(Labels.Airport, "code", toKey.substring(0,3));
                        Double maxDistance = getMaxDistance(departureAirport, arrivalAirport);

                        // Get Valid Traversals from Each Departure Airport at each step along the valid paths
                        ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> validRels = allowedCache.get(fromKey.substring(0,3) + "-" + toKey.substring(0,3));

                        // If we found valid paths from departure airport to destination airport
                        if ( !validRels.isEmpty()) {
                            // Prepare and run the second traversal
                            PathRestrictedExpander pathRestrictedExpander = new PathRestrictedExpander(toKey.substring(0, 3), timeLimit.intValue(), validRels);

                            // The cost is the distance traveled
                            RouteCostEvaluator routeCostEvaluator = new RouteCostEvaluator();

                            // Create the custom dijkstra using the path restricted expander to limit our search to only valid paths
                            PathFinder<WeightedPath> dijkstra = GraphAlgoFactory.dijkstra(pathRestrictedExpander, routeCostEvaluator, recordLimit.intValue());
                            secondTraversal(results, recordLimit.intValue(), departureAirportDay, arrivalAirport, maxDistance, dijkstra);
                        } else {
                            log.debug("No valid paths found for " + from + " to " + to + " on " + day);
                        }
                    }
                }
            }
            tx.success();
        }
        // Order the flights by # of hops, departure time, distance and the first flight code if all else is equal
        results.sort(FLIGHT_COMPARATOR);
        return results.stream();
    }

    // Return a list of valid relationship types to traverse from each airport at each step in the traversal
    // The naive approach would have just returned all rel-types in routes, but that would have allowed invalid routes
    // A smarter approach would have returned rel-types allowed from an airport anywhere along the path,
    // but one again that would have allowed invalid routes
    // This version only allows rel-types from an airport at a step in the traversal, limiting us to only valid paths
    private static ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> getValidPaths(Node departureAirport, Node arrivalAirport, Double maxDistance) {
        ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> validRels = new ArrayList<>();

        // Traverse just the Airport to Airport  FLIES_TO relationships to get possible routes for second traversal
        TraversalDescription td = graph.traversalDescription()
                .breadthFirst()
                .expand(bidirectionalFliesToExpander, ibs)
                .uniqueness(Uniqueness.NODE_PATH)
                .evaluator(Evaluators.toDepth(2));

        // Since we know the start and end of the path, we can make use of a fast bidirectional traverser
        BidirectionalTraversalDescription bidirtd = graph.bidirectionalTraversalDescription()
                .mirroredSides(td)
                .collisionEvaluator(new CollisionEvaluator());

        for (org.neo4j.graphdb.Path route : bidirtd.traverse(departureAirport, arrivalAirport)) {
            Double distance = 0D;
            for (Relationship relationship : route.relationships()) {
                distance += (Double) relationship.getProperty("distance", 25000D);
            }

            // Yes this is a bit crazy to follow but it does the job
            if (distance < maxDistance){
                String code;
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
                            RelationshipType newRelationshipType = RelationshipType.withName(newcode + "_FLIGHT");
                            valid.add(newRelationshipType);
                            validAt.put(relationshipType, valid);
                            if (validRels.size() <= count) {
                                validRels.add(count, validAt);
                            } else {
                                validRels.set(count, validAt);
                            }
                            relationshipType = newRelationshipType;
                            count++;
                    }
                }
            }
        } return validRels;
    }

    // Each path found is a valid set of flights,
    private void secondTraversal(ArrayList<MapResult> results, Integer recordLimit, Node departureAirportDay, Node arrivalAirport, Double maxDistance, PathFinder<WeightedPath> dijkstra) {
        for (org.neo4j.graphdb.Path position : dijkstra.findAllPaths(departureAirportDay, arrivalAirport)) {
            if(results.size() < recordLimit) {
                HashMap<String, Object> result = new HashMap<>();
                ArrayList<Map> flights = new ArrayList<>();
                Double distance = 0D;
                ArrayList<Node> nodes =  new ArrayList<>();
                for (Node node : position.nodes()) {
                    nodes.add(node);
                }

                for (int i = 1; i < nodes.size() - 1; i+=2) {
                    Map<String, Object> flightInfo = nodes.get(i).getAllProperties();
                    flightInfo.put("origin", ((String)nodes.get(i-1).getProperty("key")).substring(0,3));
                    flightInfo.put("destination", ((String)nodes.get(i+1).getProperty("key")).substring(0,3));
                    // These are the epoch time date fields we are removing
                    // flight should have departs_at and arrives_at with human readable date times (ex: 2016-04-28T18:30)
                    flightInfo.remove("departs");
                    flightInfo.remove("arrives");
                    flights.add(flightInfo);
                    distance += ((Number) nodes.get(i).getProperty("distance", 0)).doubleValue();
                }

                result.put("flights", flights);
                result.put("score", position.length() - 2);
                result.put("distance", distance.intValue());
                results.add(new MapResult(result));
            }
        }
    }

    // Combine the Airport Codes and days into keys to find the AirportDays quickly
    private ArrayList<String> getAirportDayKeys(List<String> from, String day) {
        ArrayList<String> departureAirportDayKeys = new ArrayList<>();
        for(String code : from) {
            String key = code + "-" + day;
            departureAirportDayKeys.add(key);
        }
        return departureAirportDayKeys;
    }
}
