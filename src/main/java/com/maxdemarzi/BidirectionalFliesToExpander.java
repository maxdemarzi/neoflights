package com.maxdemarzi;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.BranchState;

public class BidirectionalFliesToExpander implements PathExpander<Double> {

    @Override
    public Iterable<Relationship> expand(Path path, BranchState<Double> branchState) {
        return path.endNode().getRelationships(Direction.OUTGOING, RelationshipTypes.FLIES_TO);
    }

    @Override
    public PathExpander<Double> reverse() {
        return new PathExpander<Double>() {

            @Override
            public Iterable<Relationship> expand(Path path, BranchState<Double> branchState) {
                return path.endNode().getRelationships(Direction.INCOMING, RelationshipTypes.FLIES_TO);
            }

            @Override
            public PathExpander<Double> reverse() {
                return null;
            }
        };
    }
}
