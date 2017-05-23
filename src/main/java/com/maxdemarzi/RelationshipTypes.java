package com.maxdemarzi;

import org.neo4j.graphdb.RelationshipType;

enum RelationshipTypes implements RelationshipType {
    FLIES_TO,
    HAS_DAY,
    IN_GROUP
}
