package com.maxdemarzi;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.Collections;
import java.util.Iterator;

class PathRestrictedExpander implements PathExpander<Double> {
    private final String endCode;
    private final Long stopTime;
    private final RelationshipType[] allowedTypes;
    private static final Long minimumConnectTime = 30L * 60L; // 30 minutes

    public PathRestrictedExpander(String endCode, Integer stopTime, RelationshipType[] allowedTypes) {
        this.endCode = endCode;
        this.stopTime = System.currentTimeMillis() + stopTime;
        this.allowedTypes = allowedTypes;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState<Double> branchState) {
        // Stop if we are over our time limit
        if (System.currentTimeMillis() < stopTime) {

            // If at an AirportDay matching our final destination then go to the AirportDay right away
            if (path.endNode().hasLabel(Labels.AirportDay) && ((String)path.endNode().getProperty("key")).substring(0,3).equals(endCode)) {
                return path.endNode().getRelationships(Direction.INCOMING, RelationshipTypes.HAS_DAY);
            }

            if (path.length() > 2 && path.endNode().hasLabel(Labels.Leg)) {
                Iterator<Node> nodes = path.reverseNodes().iterator();
                Long departs = (Long)nodes.next().getProperty("departs");
                nodes.next(); // skip AirportDay node
                Node lastFlight = nodes.next();

                // if new flight departs too soon after we arrive, skip it
                if (((Long)lastFlight.getProperty("arrives") + minimumConnectTime) > departs) {
                    return Collections.emptyList();
                }
            }

            return path.endNode().getRelationships(Direction.OUTGOING, allowedTypes);

        }

        return Collections.emptyList();
    }

    @Override
    public PathExpander<Double> reverse() {
        return null;
    }
}
