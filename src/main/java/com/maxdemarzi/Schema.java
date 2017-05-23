package com.maxdemarzi;

import com.maxdemarzi.results.ListResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Schema {

    // This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Description("com.maxdemarzi.generateSchema() | Creates schema for SecurityUser and SecurityGroup")
    @Procedure(name = "com.maxdemarzi.generateSchema", mode = Mode.SCHEMA)
    public Stream<ListResult> generateSchema() throws IOException {
        List<Object> results = new ArrayList<>();

        org.neo4j.graphdb.schema.Schema schema = db.schema();
        if (!schema.getConstraints(Labels.Airport).iterator().hasNext()) {
            schema.constraintFor(Labels.Airport)
                    .assertPropertyIsUnique("code")
                    .create();
            results.add("(:Airport {code}) constraint created");
        }

        if (!schema.getConstraints(Labels.AirportDay).iterator().hasNext()) {
            schema.constraintFor(Labels.AirportDay)
                    .assertPropertyIsUnique("key")
                    .create();

            results.add("(:AirportDay {key}) constraint created");
        }

        log.info("Flight Search Schema Created");

        return Stream.of(new ListResult(results));
    }
}
