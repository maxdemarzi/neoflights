package com.maxdemarzi;

import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

class TestUtils {

    static ArrayList getResultRow(HTTP.Response response) {
        Map actual = response.content();
        ArrayList results = (ArrayList)actual.get("results");
        HashMap result = (HashMap)results.get(0);
        ArrayList<Map> data = (ArrayList)result.get("data");
        ArrayList<Map> values = new ArrayList();
        data.forEach((value) -> values.add((Map)((ArrayList) value.get("row")).get(0)));
        return values;
    }

    static String getResultString(HTTP.Response response) {
        Map actual = response.content();
        ArrayList results = (ArrayList)actual.get("results");
        HashMap result = (HashMap)results.get(0);
        ArrayList<Map> data = (ArrayList)result.get("data");
        return (String) ((ArrayList) data.get(0).get("row")).get(0);
    }


    static final HashMap<String, Object> SCHEMA = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.aa.generateSchema()");
            }});
        }});
    }};

}
