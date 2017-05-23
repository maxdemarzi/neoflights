package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.maxdemarzi.TestUtils.SCHEMA;
import static com.maxdemarzi.TestUtils.getResultString;
import static junit.framework.TestCase.assertTrue;

public class ImportFlightsTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Schema.class)
            .withProcedure(Imports.class);

    @Test
    public void shouldImportFlights() {
        HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), SCHEMA);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        String message = getResultString(response);

        assertTrue(message.startsWith(EXPECTED));
    }


    private static final HashMap<String, Object> PARAMS = new HashMap<String, Object>(){{
        put("file", "/Users/maxdemarzi/Projects/neoflights/src/main/resources/data/flights.csv");
    }};

    private static final HashMap<String, Object> QUERY = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.maxdemarzi.import.flights({file})");
                put("parameters", PARAMS);
            }});
        }});
    }};

    private static final String EXPECTED = "366 Flights imported in ";

}
