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

public class TwoStopTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withProcedure(Schema.class)
            .withProcedure(Flights.class);

    @Test
    public void shouldFindTwoStopRoute() {
        HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), SCHEMA);
        HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), CLEAR);
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        ArrayList row = getResultRow(response);
        assertEquals(ANSWER_LIST, row);
    }

    private static final String MODEL_STATEMENT =
            // Fly from Dallas to Haneda Airport in Tokyo
            "CREATE (iah:Airport {code:'IAH', latitude: 0.5233272799368780000000000, longitude: -1.66402114953545000000})" +
            "CREATE (ord:Airport {code:'ORD', latitude: 0.7326649793031630000000000, longitude: -1.53422683082880000000})" +
            "CREATE (ewr:Airport {code:'EWR', latitude: 0.7102181058677910000000000, longitude: -1.29448646552014000000})" +
            "CREATE (dfw:Airport {code:'DFW', latitude: 0.5741599944012120000000000, longitude: -1.69363356917762000000})" +
            "CREATE (hnd:Airport {code:'HND', latitude: 0.620464, longitude: 2.439733})" +
            "CREATE (dfw)-[:FLIES_TO {distance:225.0}]->(iah)" +
            "CREATE (iah)-[:FLIES_TO {distance:718.0}]->(ord)" +
            "CREATE (iah)-[:FLIES_TO {distance:1416.0}]->(ewr)" +
            "CREATE (ord)-[:FLIES_TO {distance:6296.0}]->(hnd)" +
            "CREATE (ewr)-[:FLIES_TO {distance:6731.0}]->(hnd)" +
            "CREATE (dfw_20150901:AirportDay {key:'DFW-2015-09-01'})" +
            "CREATE (iah_20150901:AirportDay {key:'IAH-2015-09-01'})" +
            "CREATE (ord_20150901:AirportDay {key:'ORD-2015-09-01'})" +
            "CREATE (ewr_20150901:AirportDay {key:'EWR-2015-09-01'})" +
            "CREATE (hnd_20150902:AirportDay {key:'HND-2015-09-02'})" +
            "CREATE (dfw)-[:HAS_DAY]->(dfw_20150901)" +
            "CREATE (iah)-[:HAS_DAY]->(iah_20150901)" +
            "CREATE (ord)-[:HAS_DAY]->(ord_20150901)" +
            "CREATE (ewr)-[:HAS_DAY]->(ewr_20150901)" +
            "CREATE (hnd)-[:HAS_DAY]->(hnd_20150902)" +
            "CREATE (leg0:Leg {code:'NEO-0', departs:1441101600, arrives:1441105200, distance:225})" +
            "CREATE (leg1:Leg {code:'NEO-1', departs:1441108800, arrives:1441119600, distance:718})" +
            "CREATE (leg2:Leg {code:'NEO-2', departs:1441108800, arrives:1441123200, distance:1416})" +
            "CREATE (leg3:Leg {code:'NEO-3', departs:1441123200, arrives:1441177200, distance:6296})" +
            "CREATE (leg4:Leg {code:'NEO-4', departs:1441130400, arrives:1441180800, distance:6731})" +
            "CREATE (dfw_20150901)-[:IAH_FLIGHT]->(leg0)" +
            "CREATE (leg0)-[:IAH_FLIGHT]->(iah_20150901)" +
            "CREATE (iah_20150901)-[:ORD_FLIGHT]->(leg1)" +
            "CREATE (leg1)-[:ORD_FLIGHT]->(ord_20150901)" +
            "CREATE (ord_20150901)-[:HND_FLIGHT]->(leg3)" +
            "CREATE (leg3)-[:HND_FLIGHT]->(hnd_20150902)" +
            "CREATE (iah_20150901)-[:EWR_FLIGHT]->(leg2)" +
            "CREATE (leg2)-[:EWR_FLIGHT]->(ewr_20150901)" +
            "CREATE (ewr_20150901)-[:HND_FLIGHT]->(leg4)" +
            "CREATE (leg4)-[:HND_FLIGHT]->(hnd_20150902)";

    private static final HashMap<String, Object> CLEAR = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.maxdemarzi.clear_flight_cache()");
            }});
        }});
    }};

    private static final HashMap<String, Object> PARAMS = new HashMap<String, Object>(){{
        put("from", new ArrayList<String>() {{ add("DFW"); }});
        put("to",  new ArrayList<String>() {{ add("HND"); }});
        put("day", "2015-09-01");
    }};

    private static final HashMap<String, Object> QUERY = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.maxdemarzi.flights({from}, {to}, {day})");
                put("parameters", PARAMS);
            }});
        }});
    }};

    private static final HashMap<String, Object> LEG0_MAP = new HashMap<String, Object>(){{
        put("code","NEO-0");
        put("distance", 225);
        put("origin", "DFW");
        put("destination", "IAH");
    }};

    private static final HashMap<String, Object> LEG1_MAP = new HashMap<String, Object>(){{
        put("code","NEO-1");
        put("distance", 718);
        put("origin", "IAH");
        put("destination", "ORD");
    }};

    private static final HashMap<String, Object> LEG2_MAP = new HashMap<String, Object>(){{
        put("code","NEO-3");
        put("distance", 6296);
        put("origin", "ORD");
        put("destination", "HND");
    }};

    private static final ArrayList<HashMap> FLIGHT_LIST1 = new ArrayList<HashMap>(){{
        add(LEG0_MAP);
        add(LEG1_MAP);
        add(LEG2_MAP);
    }};

    private static final HashMap<String, Object> ANSWER_MAP1 = new HashMap<String, Object>(){{
        put("flights", FLIGHT_LIST1);
        put("score", 5);
        put("distance", 7239);
    }};

    private static final HashMap<String, Object> LEG3_MAP = new HashMap<String, Object>(){{
        put("code","NEO-2");
        put("distance", 1416);
        put("origin", "IAH");
        put("destination", "EWR");
    }};

    private static final HashMap<String, Object> LEG4_MAP = new HashMap<String, Object>(){{
        put("code","NEO-4");
        put("distance", 6731);
        put("origin", "EWR");
        put("destination", "HND");
    }};

    private static final ArrayList<HashMap> FLIGHT_LIST2 = new ArrayList<HashMap>(){{
        add(LEG0_MAP);
        add(LEG3_MAP);
        add(LEG4_MAP);
    }};

    private static final HashMap<String, Object> ANSWER_MAP2 = new HashMap<String, Object>(){{
        put("flights", FLIGHT_LIST2);
        put("score", 5);
        put("distance", 8372);
    }};

    private static final ArrayList<HashMap> ANSWER_LIST = new ArrayList<HashMap>(){{
        add(ANSWER_MAP1);
        add(ANSWER_MAP2);
    }};
}
