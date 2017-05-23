package com.maxdemarzi;

import com.maxdemarzi.results.StringResult;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.time.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Imports {

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    private static final int TRANSACTION_LIMIT = 1000;

    @Description("com.maxdemarzi.import.airports(file) | Import Airports")
    @Procedure(name = "com.maxdemarzi.import.airports", mode = Mode.WRITE)

    public Stream<StringResult> importAirports(@Name("file") String file) throws IOException {
        long start = System.nanoTime();
        Reader in = new FileReader("/" + file);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        int count = 0;
        try {
            for (CSVRecord record : records) {
                count++;
                String code = record.get("Code");
                String fplat = record.get("Lat");
                String fplon =  record.get("Lon");
                String country =  record.get("Country");

                Double latitude = Double.parseDouble(fplat);
                Double longitude = Double.parseDouble(fplon);

                Node airport = db.findNode(Labels.Airport, "code", code);
                if (airport == null) {
                    airport = db.createNode(Labels.Airport);
                    airport.setProperty("code", code);
                    airport.setProperty("latitude", latitude);
                    airport.setProperty("longitude", longitude);
                    airport.setProperty("country", country);
                }

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } finally {
            tx.close();
        }

        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

        return Stream.of(new StringResult(count + " Airports imported in " + timeTaken + " Seconds"));
    }

    @Description("com.maxdemarzi.import.flights(file) | Import Flights")
    @Procedure(name = "com.maxdemarzi.import.flights", mode = Mode.WRITE)

    public Stream<StringResult> importFlights(@Name("file") String file) throws IOException {
        long start = System.nanoTime();
        Reader in = new FileReader("/" + file);
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withHeader().parse(in);

        Transaction tx = db.beginTx();
        int count = 0;
        try {

            for (CSVRecord record : records) {
                count++;

                // Airports
                String departureCity = record.get("DepartureCity");
                String arrivalCity = record.get("ArrivalCity");
                db.execute("MERGE (a:Airport {code:{code}})",
                        Collections.singletonMap("code", departureCity));
                db.execute("MERGE (a:Airport {code:{code}})",
                        Collections.singletonMap("code", arrivalCity));

                // Legs and AirportDays
                String airlineCode = record.get("AirlineCode");
                String flightNumber = record.get("FlightNumber");
                String departureTime = String.format("%04d", Integer.parseInt(record.get("DepartureTime")));
                String arrivalTime = String.format("%04d", Integer.parseInt(record.get("ArrivalTime")));
                String variationDepartureTimeCode = record.get("VariationDepartureTimeCode");
                String variationArrivalTimeCode = record.get("VariationArrivalTimeCode");

                Integer variationDepartureTimeCodeOffset = Integer.parseInt(variationDepartureTimeCode);
                Integer variationArrivalTimeCodeOffset = Integer.parseInt(variationArrivalTimeCode);

                LocalTime departureLocalTime = LocalTime.of(
                        Integer.parseInt(departureTime.substring(0, departureTime.length() - 2)) % 24,
                        Integer.parseInt(departureTime.substring(departureTime.length() - 2, departureTime.length())));

                LocalTime arrivalLocalTime = LocalTime.of(
                        Integer.parseInt(arrivalTime.substring(0, arrivalTime.length() - 2)) % 24,
                        Integer.parseInt(arrivalTime.substring(arrivalTime.length() - 2, arrivalTime.length())));

                String departureTimezone = String.format("%+05d", Integer.parseInt(record.get("DepartureTimezone")));
                String arrivalTimezone = String.format("%+05d", Integer.parseInt(record.get("ArrivalTimezone")));

                ZoneOffset departureZoneOffset = ZoneOffset.of(
                        departureTimezone.substring(0, 3) +
                                ":" +
                                departureTimezone.substring(3, 5));

                ZoneOffset arrivalZoneOffset = ZoneOffset.of(
                        arrivalTimezone.substring(0, 3) +
                                ":" +
                                arrivalTimezone.substring(3, 5));

                String dayOfOperationMonday = record.get("DayOfOperationMonday");
                String dayOfOperationTuesday = record.get("DayOfOperationTuesday");
                String dayOfOperationWednesday = record.get("DayOfOperationWednesday");
                String dayOfOperationThursday = record.get("DayOfOperationThursday");
                String dayOfOperationFriday = record.get("DayOfOperationFriday");
                String dayOfOperationSaturday = record.get("DayOfOperationSaturday");
                String dayOfOperationSunday = record.get("DayOfOperationSunday");

                Set<Integer> daysOfOperation = new HashSet<>();
                if (!dayOfOperationMonday.isEmpty()) { daysOfOperation.add(1); }
                if (!dayOfOperationTuesday.isEmpty()) { daysOfOperation.add(2); }
                if (!dayOfOperationWednesday.isEmpty()) { daysOfOperation.add(3); }
                if (!dayOfOperationThursday.isEmpty()) { daysOfOperation.add(4); }
                if (!dayOfOperationFriday.isEmpty()) { daysOfOperation.add(5); }
                if (!dayOfOperationSaturday.isEmpty()) { daysOfOperation.add(6); }
                if (!dayOfOperationSunday.isEmpty()) { daysOfOperation.add(7); }

                String effectiveDate = record.get("EffectiveDate");
                String[] effectiveDatePieces = effectiveDate.split("/");
                LocalDate effectiveLocalDate = LocalDate.of(2000 + Integer.parseInt(effectiveDatePieces[2]),
                        Integer.parseInt(effectiveDatePieces[0]),
                        Integer.parseInt(effectiveDatePieces[1]));

                String discontinueDate = record.get("DiscontinueDate");
                String[] discontinueDatePieces = discontinueDate.split("/");
                LocalDate discontinueLocalDate = LocalDate.of(2000 + Integer.parseInt(discontinueDatePieces[2]),
                        Integer.parseInt(discontinueDatePieces[0]),
                        Integer.parseInt(discontinueDatePieces[1]));

                Period daysBetween = Period.between(effectiveLocalDate, discontinueLocalDate);
                for (int i = 0; i < daysBetween.getDays(); i++) {
                    if (daysOfOperation.contains(effectiveLocalDate.plusDays(i).getDayOfWeek().getValue())) {

                        LocalDateTime departureLocalDateTime = LocalDateTime.of(effectiveLocalDate.plusDays(i + variationDepartureTimeCodeOffset), departureLocalTime);
                        OffsetDateTime departureDateTime = OffsetDateTime.of(departureLocalDateTime, departureZoneOffset);
                        Timestamp departureTimestamp = Timestamp.valueOf(departureDateTime.atZoneSameInstant(ZoneId.of("Z")).toLocalDateTime());

                        LocalDateTime arrivalLocalDateTime = LocalDateTime.of(effectiveLocalDate.plusDays(i + variationArrivalTimeCodeOffset), arrivalLocalTime);
                        OffsetDateTime arrivalDateTime = OffsetDateTime.of(arrivalLocalDateTime, arrivalZoneOffset);
                        Timestamp arrivalTimestamp = Timestamp.valueOf(arrivalDateTime.atZoneSameInstant(ZoneId.of("Z")).toLocalDateTime());

                        // AirportDay
                        String departureKey = departureCity + "-" + departureLocalDateTime.toLocalDate();
                        Node departureAirportDayNode = (Node)db.execute("MERGE (a:AirportDay {key:{key}}) RETURN a",
                                Collections.singletonMap("key", departureKey)
                        ).columnAs("a").next();

                        String arrivalKey = arrivalCity + "-" + arrivalLocalDateTime.toLocalDate();
                        Node arrivalAirportDayNode = (Node)db.execute("MERGE (a:AirportDay {key:{key}}) RETURN a",
                                Collections.singletonMap("key",arrivalKey)
                        ).columnAs("a").next();

                        db.execute("MATCH (a:Airport {code:{code}}), (ad:AirportDay {key:{key}}) MERGE (a)-[:HAS_DAY]->(ad)",
                                new HashMap<String, Object>() {{
                                    put("code", departureCity);
                                    put("key", departureKey); }});

                        db.execute("MATCH (a:Airport {code:{code}}), (ad:AirportDay {key:{key}}) MERGE (a)-[:HAS_DAY]->(ad)",
                                new HashMap<String, Object>() {{
                                    put("code", arrivalCity);
                                    put("key", arrivalKey); }});

                        Node leg = (Node)db.execute("CREATE (l:Leg { code:{code}, departs:{departs}, arrives:{arrives}, departs_at:{departs_at}, arrives_at:{arrives_at}, distance:{distance} }) RETURN l ",
                                new HashMap<String, Object>() {{
                                    put("code", airlineCode + "-" + flightNumber);
                                    put("departs", departureTimestamp.getTime()/1000);
                                    put("arrives", arrivalTimestamp.getTime()/1000);
                                    put("departs_at", departureLocalDateTime.toString());
                                    put("arrives_at", arrivalLocalDateTime.toString());
                                    put("distance", Integer.parseInt(record.get("FlightDistance")));
                                }}).columnAs("l").next();

                        departureAirportDayNode.createRelationshipTo(leg, RelationshipType.withName(arrivalCity + "_FLIGHT"));
                        leg.createRelationshipTo(arrivalAirportDayNode, RelationshipType.withName(arrivalCity + "_FLIGHT"));

                    }
                }

                if (count % TRANSACTION_LIMIT == 0) {
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }
            }

            tx.success();
        } catch (Exception e) {
            System.out.println("Error on line: " +  count);
            e.printStackTrace();
        }
        finally {
            tx.close();
        }

        long timeTaken = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start);

        return Stream.of(new StringResult(count + " Flights imported in " + timeTaken + " Seconds"));
    }
}
