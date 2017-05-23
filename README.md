# Neo Flights
POC Flight Search using Neo4j


# Instructions

1. Build it:

        mvn clean package

2. Copy target/neo-flights-1.0-SNAPSHOT.jar to the plugins/ directory of your Neo4j server.

3. (Re)Start Neo4j server.

4. Create the Schema:

        CALL com.maxdemarzi.generateSchema();

5. Import Airports:

        CALL com.maxdemarzi.import.airports("path to your airport csv file, see below for example");
        CALL com.maxdemarzi.import.airports("/Users/maxdemarzi/Projects/neoflights/src/main/resources/data/airports.csv")


6. Import flights: 

        CALL com.maxdemarzi.import.flights("path to your flight csv file, see below for example");
        CALL com.maxdemarzi.import.flights("/Users/maxdemarzi/Projects/neoflights/src/main/resources/data/flights.csv")

7. Connect the airports via the flights.
        
        MATCH (a1:Airport)-[:HAS_DAY]->(ad1:AirportDay)-->
        (l:Leg)-->(ad2:AirportDay)<-[:HAS_DAY]-(a2:Airport)
        WHERE a1 <> a2
        WITH a1,  AVG(l.distance) AS avg_distance, a2, COUNT(*) AS flights
        MERGE (a1)-[r:FLIES_TO]->(a2)
        SET r.distance = avg_distance, r.flights = flights

8. OR you could skip 5-7 and just create the data like this:

        CREATE (iah:Airport {code:'IAH', latitude: 0.5233272799368780000000000, longitude: -1.66402114953545000000})
        CREATE (ord:Airport {code:'ORD', latitude: 0.7326649793031630000000000, longitude: -1.53422683082880000000})
        CREATE (ewr:Airport {code:'EWR', latitude: 0.7102181058677910000000000, longitude: -1.29448646552014000000})
        CREATE (dfw:Airport {code:'DFW', latitude: 0.5741599944012120000000000, longitude: -1.69363356917762000000})
        CREATE (hnd:Airport {code:'HND', latitude: 0.620464, longitude: 2.439733})
        CREATE (dfw)-[:FLIES_TO {distance:225.0}]->(iah)
        CREATE (iah)-[:FLIES_TO {distance:718.0}]->(ord)
        CREATE (iah)-[:FLIES_TO {distance:1416.0}]->(ewr)
        CREATE (ord)-[:FLIES_TO {distance:6296.0}]->(hnd)
        CREATE (ewr)-[:FLIES_TO {distance:6731.0}]->(hnd)
        CREATE (dfw_20150901:AirportDay {key:'DFW-2015-09-01'})
        CREATE (iah_20150901:AirportDay {key:'IAH-2015-09-01'})
        CREATE (ord_20150901:AirportDay {key:'ORD-2015-09-01'})
        CREATE (ewr_20150901:AirportDay {key:'EWR-2015-09-01'})
        CREATE (hnd_20150902:AirportDay {key:'HND-2015-09-02'})
        CREATE (dfw)-[:HAS_DAY]->(dfw_20150901)
        CREATE (iah)-[:HAS_DAY]->(iah_20150901)
        CREATE (ord)-[:HAS_DAY]->(ord_20150901)
        CREATE (ewr)-[:HAS_DAY]->(ewr_20150901)
        CREATE (hnd)-[:HAS_DAY]->(hnd_20150902)
        CREATE (leg0:Leg {code:'AA-0', departs:1441101600, arrives:1441105200, distance:225})
        CREATE (leg1:Leg {code:'AA-1', departs:1441108800, arrives:1441119600, distance:718})
        CREATE (leg2:Leg {code:'AA-2', departs:1441108800, arrives:1441123200, distance:1416})
        CREATE (leg3:Leg {code:'AA-3', departs:1441123200, arrives:1441177200, distance:6296})
        CREATE (leg4:Leg {code:'AA-4', departs:1441130400, arrives:1441180800, distance:6731})
        CREATE (dfw_20150901)-[:IAH_FLIGHT]->(leg0)
        CREATE (leg0)-[:IAH_FLIGHT]->(iah_20150901)
        CREATE (iah_20150901)-[:ORD_FLIGHT]->(leg1)
        CREATE (leg1)-[:ORD_FLIGHT]->(ord_20150901)
        CREATE (ord_20150901)-[:HND_FLIGHT]->(leg3)
        CREATE (leg3)-[:HND_FLIGHT]->(hnd_20150902)
        CREATE (iah_20150901)-[:EWR_FLIGHT]->(leg2)
        CREATE (leg2)-[:EWR_FLIGHT]->(ewr_20150901)
        CREATE (ewr_20150901)-[:HND_FLIGHT]->(leg4)
        CREATE (leg4)-[:HND_FLIGHT]->(hnd_20150902);
                
                    
9. Try a query:

        CALL com.maxdemarzi.flights('SEA','ORD','2016-04-28'); (if you imported)
        CALL com.maxdemarzi.flights('DFW','ORD','2015-09-01'); (if you manually created it)