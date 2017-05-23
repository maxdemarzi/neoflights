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
import static junit.framework.TestCase.assertEquals;

public class ImportAirportsTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Schema.class)
            .withProcedure(Imports.class);

    @Test
    public void shouldImportAirports() {
        HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), SCHEMA);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        String message = getResultString(response);

        assertEquals(EXPECTED, message);
    }


    private static final HashMap<String, Object> PARAMS = new HashMap<String, Object>(){{
        put("file", "/Users/maxdemarzi/Projects/neoflights/src/main/resources/data/airports.csv");
    }};

    private static final HashMap<String, Object> QUERY = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.maxdemarzi.import.airports({file})");
                put("parameters", PARAMS);
            }});
        }});
    }};

    private static final String EXPECTED = "14 Airports imported in 0 Seconds";

}
