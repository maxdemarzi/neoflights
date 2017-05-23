package com.maxdemarzi;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.BranchState;

import java.util.*;

class PathRestrictedExpander implements PathExpander<Double> {
    private final String endCode;
    private final long stopTime;
    private final ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> validRels;

    // TODO: 3/8/17 Make this part of the query
    private static final Long minimumConnectTime = 30L * 60L; // 30 minutes

    public PathRestrictedExpander(String endCode, long stopTime, ArrayList<HashMap<RelationshipType, Set<RelationshipType>>> validRels) {
        this.endCode = endCode;
        this.stopTime = System.currentTimeMillis() + stopTime;
        this.validRels = validRels;
    }

    @Override
    public Iterable<Relationship> expand(Path path, BranchState<Double> branchState) {
        // Stop if we are over our time limit
        if (System.currentTimeMillis() < stopTime * 100000) {
            if (path.length() < 8) {

                if (((path.length() % 2) == 0) && ((String)path.endNode().getProperty("key")).substring(0,3).equals(endCode)) {
                    return path.endNode().getRelationships(Direction.INCOMING, RelationshipTypes.HAS_DAY);
                }

                if (path.length() > 2 && ((path.length() % 2) == 1) ) {
                    Iterator<Node> nodes = path.reverseNodes().iterator();
                    Long departs = (Long)nodes.next().getProperty("departs");
                    nodes.next(); // skip AirportDay node
                    Node lastFlight = nodes.next();
                    if (((Long)lastFlight.getProperty("arrives") + minimumConnectTime) > departs) {
                        return Collections.emptyList();
                    }
                }

                if (path.length() < 2) {
                    RelationshipType firstRelationshipType = RelationshipType.withName(((String)path.startNode().getProperty("key")).substring(0,3) + "_FLIGHT");
                    RelationshipType[] valid = validRels.get(0).get(firstRelationshipType).toArray(new RelationshipType[validRels.get(0).get(firstRelationshipType).size()]);
                    return path.endNode().getRelationships(Direction.OUTGOING, valid);
                } else {
                    int location = path.length() / 2;

                    if (((path.length() % 2) == 0) ) {
                        return path.endNode().getRelationships(Direction.OUTGOING, validRels.get(location).get(path.lastRelationship().getType()).toArray(new RelationshipType[validRels.get(location).get(path.lastRelationship().getType()).size()]));
                    } else {
                        Iterator<Relationship> iter = path.reverseRelationships().iterator();
                        iter.next();
                        RelationshipType lastRelationshipType = iter.next().getType();
                        return path.endNode().getRelationships(Direction.OUTGOING, validRels.get(location).get(lastRelationshipType).toArray(new RelationshipType[validRels.get(location).get(lastRelationshipType).size()]));
                    }
                }
            }
        }

        return Collections.emptyList();
    }

    @Override
    public PathExpander<Double> reverse() {
        return null;
    }
}