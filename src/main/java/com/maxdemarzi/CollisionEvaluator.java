package com.maxdemarzi;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;

public class CollisionEvaluator implements Evaluator {
    @Override
    public Evaluation evaluate(Path path) {
        return Evaluation.INCLUDE_AND_PRUNE;
    }
}
