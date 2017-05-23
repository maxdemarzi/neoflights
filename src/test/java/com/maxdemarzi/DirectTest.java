package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.maxdemarzi.TestUtils.SCHEMA;
import static com.maxdemarzi.TestUtils.getResultRow;
import static junit.framework.TestCase.assertEquals;

public class DirectTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withProcedure(Schema.class)
            .withProcedure(Flights.class);

    @Test
    public void shouldFindDirectRoute() {
        HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), SCHEMA);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        ArrayList row = getResultRow(response);

        assertEquals(ANSWER_LIST, row);
    }


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

    private static final String MODEL_STATEMENT =
            // IAH to EWR Direct via Ohare
            "CREATE (iah:Airport {code:'IAH', latitude: 0.5233272799368780000000000, longitude: -1.66402114953545000000})" +
                    "CREATE (ord:Airport {code:'ORD', latitude: 0.7326649793031630000000000, longitude: -1.53422683082880000000})" +
                    "CREATE (ewr:Airport {code:'EWR', latitude:0.7102181058677910000000000, longitude: -1.29448646552014000000})" +
                    "CREATE (iah)-[:FLIES_TO {distance:926.0}]->(ord)" +
                    "CREATE (ord)-[:FLIES_TO {distance:718.0}]->(ewr)" +
                    "CREATE (iah_20150506:AirportDay {key:'IAH-2015-05-06'})" +
                    "CREATE (ord_20150506:AirportDay {key:'ORD-2015-05-06'})" +
                    "CREATE (ewr_20150506:AirportDay {key:'EWR-2015-05-06'})" +
                    "CREATE (iah)-[:HAS_DAY]->(iah_20150506)" +
                    "CREATE (ord)-[:HAS_DAY]->(ord_20150506)" +
                    "CREATE (ewr)-[:HAS_DAY]->(ewr_20150506)" +
                    "CREATE (leg1:Leg {code:'NEO-690', departs:1430916420, arrives:1430925900, distance:926})" + // 5/6/15@12:47pm-3:25pm
                    "CREATE (leg2:Leg {code:'NEO-690', departs:1430928000, arrives:1430939760, distance:718})" + // 5/6/15@4:00pm-7:16pm
                    "CREATE (iah_20150506)-[:ORD_FLIGHT]->(leg1)" +
                    "CREATE (leg1)-[:ORD_FLIGHT]->(ord_20150506)" +
                    "CREATE (ord_20150506)-[:EWR_FLIGHT]->(leg2)" +
                    "CREATE (leg2)-[:EWR_FLIGHT]->(ewr_20150506)";


    private static final Map<String, Object> LEG1_MAP = new HashMap<String, Object>(){{
        put("code","NEO-690");
        put("distance", 926);
        put("origin", "IAH");
        put("destination", "ORD");
    }};

    private static final Map<String, Object> LEG2_MAP = new HashMap<String, Object>(){{
        put("code","NEO-690");
        put("distance", 718);
        put("origin", "ORD");
        put("destination", "EWR");
    }};

    private static final List<Map> FLIGHT_LIST = new ArrayList<Map>(){{
        add(LEG1_MAP);
        add(LEG2_MAP);
    }};

    private static final Map<String, Object> ANSWER_MAP = new HashMap<String, Object>(){{
        put("flights", FLIGHT_LIST);
        put("score", 3);
        put("distance", 1644);
    }};

    private static final List<Map> ANSWER_LIST = new ArrayList<Map>(){{
        add(ANSWER_MAP);
    }};

}
