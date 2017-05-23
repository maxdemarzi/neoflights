package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.maxdemarzi.TestUtils.SCHEMA;
import static com.maxdemarzi.TestUtils.getResultRow;
import static junit.framework.TestCase.assertEquals;

public class NonStopTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withProcedure(Schema.class)
            .withProcedure(Flights.class);

    @Test
    public void shouldFindNonStopRoute() {
        HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), SCHEMA);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        ArrayList row = getResultRow(response);

        assertEquals(ANSWER_LIST, row);
    }

    private static final String MODEL_STATEMENT =
            // IAH to EWR Non Stop
            "CREATE (iah:Airport {code:'IAH', latitude: 0.5233272799368780000000000, longitude: -1.66402114953545000000})" +
            "CREATE (ewr:Airport {code:'EWR', latitude: 0.7102181058677910000000000, longitude: -1.29448646552014000000})" +
            "CREATE (iah)-[:FLIES_TO {distance:926.0}]->(ewr)" +
            "CREATE (iah_20150506:AirportDay {key:'IAH-2015-05-06'})" +
            "CREATE (ewr_20150506:AirportDay {key:'EWR-2015-05-06'})" +
            "CREATE (iah)-[:HAS_DAY]->(iah_20150506)" +
            "CREATE (ewr)-[:HAS_DAY]->(ewr_20150506)" +
            "CREATE (leg1:Leg {code:'NEO-690', departs:1430916420, arrives:1430925900, distance:926})" + // 5/6/15@12:47pm-3:25pm
            "CREATE (iah_20150506)-[:EWR_FLIGHT]->(leg1)" +
            "CREATE (leg1)-[:EWR_FLIGHT]->(ewr_20150506)";

    private static final HashMap<String, Object> PARAMS = new HashMap<String, Object>(){{
        put("from", "IAH");
        put("to", "EWR");
        put("day", "2015-05-06");
    }};

    private static final HashMap<String, Object> QUERY = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.maxdemarzi.flights({from}, {to}, {day})");
                put("parameters", PARAMS);
            }});
        }});
    }};
    
    private static final HashMap<String, Object> LEG1_MAP = new HashMap<String, Object>(){{
        put("code","NEO-690");
        put("distance", 926);
        put("origin", "IAH");
        put("destination", "EWR");
    }};

    private static final ArrayList<HashMap> FLIGHT_LIST = new ArrayList<HashMap>(){{
        add(LEG1_MAP);
    }};

    private static final HashMap<String, Object> ANSWER_MAP = new HashMap<String, Object>(){{
        put("flights", FLIGHT_LIST);
        put("score", 1);
        put("distance", 926);
    }};

    private static final ArrayList<HashMap> ANSWER_LIST = new ArrayList<HashMap>(){{
        add(ANSWER_MAP);
    }};
}
