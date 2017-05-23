package com.maxdemarzi;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

class RouteCostEvaluator implements CostEvaluator<Double> {
    @Override
    public Double getCost(Relationship relationship, Direction direction) {
        return ((Number) relationship.getEndNode().getProperty("distance", 0)).doubleValue();
    }
}
