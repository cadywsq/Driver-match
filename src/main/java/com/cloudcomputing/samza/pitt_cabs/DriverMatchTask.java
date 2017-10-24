package com.cloudcomputing.samza.pitt_cabs;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider
 * to driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private KeyValueStore<String, Map<String, Object>> driverLocations;
    private Map<Integer, ArrayList<Integer>> driverRatioMap;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        //Initialize stuff (maybe the kv stores?)
        driverLocations = (KeyValueStore<String, Map<String, Object>>) context.getStore("driver-loc");
        driverRatioMap = new HashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a blockId will arrive
        // at one task only, thereby enabling you to do stateful stream processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            processDriverLoc((Map<String, Object>) envelope.getMessage());
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            processEvent((Map<String, Object>) envelope.getMessage(), collector);
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    public void processDriverLoc(Map<String, Object> message) {
        if (!message.get("type").equals("DRIVER_LOCATION")) {
            throw new IllegalStateException("Unexpected event type on follows stream: " + message.get("type"));
        }
        int blockId = (int) (message.get("blockId"));
        int driverId = (int) (message.get("driverId"));
        Map<String, Object> theMsg = driverLocations.get(blockId + ":" + driverId);

        if (theMsg != null) {
            theMsg.put("latitude", message.get("latitude"));
            theMsg.put("longitude", message.get("longitude"));
        }
    }

    public void processEvent(Map<String, Object> message, MessageCollector collector) {
        String type = (String) (message.get("type"));
        if (type.equals("DRIVER_LOCATION")) {
            throw new IllegalStateException("Unexpected event type on follows stream: " + message.get("type"));
        }
        if (type.equals("RIDE_REQUEST")) {
            fanOut(message, collector);
        } else {
            int blockId = (int) message.get("blockId");
            int driverId = (int) (message.get("driverId"));
            String status = (String) (message.get("status"));

            if (type.equals("RIDE_COMPLETE") || (type.equals("ENTERING_BLOCK") && status.equals("AVAILABLE"))) {
                driverLocations.put(blockId + ":" + driverId, message);
            } else {
                driverLocations.delete(blockId + ":" + driverId);
            }
        }
    }

    private void fanOut(Map<String, Object> message, MessageCollector collector) {
        int clientId = (int) message.get("clientId");
        int blockId = (int) (message.get("blockId"));
        // Colon is used as separator, and semicolon is lexicographically after colon
        KeyValueIterator<String, Map<String, Object>> drivers = driverLocations.range(blockId + ":", blockId + ";");

//        int driverId = getMatchedDriver(message, drivers);
        int clatitude = (int) message.get("latitude");
        int clongitude = (int) message.get("longitude");
        String genderPrefer = (String) (message.get("gender_preference"));
        double maxScore = 0;

        int driverId = 0;
        int availableCount = 0;
        while (drivers.hasNext()) {
            Entry<String, Map<String, Object>> driver = drivers.next();
            availableCount++;
            driverId = Integer.valueOf(driver.getKey().split(":")[1]);
            Map<String, Object> driverMsg = driver.getValue();
            // Get parameters
            int dLatitude = (int) driverMsg.get("latitude");
            int dLongitude = (int) driverMsg.get("longitude");
            String gender = (String) (driverMsg.get("gender"));
            double rating = (double) driverMsg.get("rating");
            int salary = (int) driverMsg.get("salary");
            // Calculate scores
            double distance = getDistance(clatitude, clongitude, dLatitude, dLongitude);
            double genderScore = getGenderScore(genderPrefer, gender);
            double score = getScore(distance, genderScore, rating, salary);

            if (score > maxScore) {
                maxScore = score;
            }
        }
        driverLocations.delete(blockId + ":" + driverId);

        double spf = 1.0;
        ArrayList<Integer> driverRatio = driverRatioMap.get(blockId);
        if (driverRatio != null && driverRatio.size() == 4) {
            spf = getSPF(driverRatio, availableCount);
            driverRatio.remove(3);
        } else {
            if (driverRatio == null) {
                driverRatio = new ArrayList<>();
                driverRatioMap.put(blockId, driverRatio);
            }
        }
        driverRatio.add(0, availableCount);

        Map<String, Object> output = new HashMap<>();
        output.put("clientId", clientId);
        output.put("driverId", driverId);
        output.put("priceFactor", spf);

        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM, output));
        drivers.close();
    }

//    private int getMatchedDriver(Map<String, Object> message, KeyValueIterator<String, Map<String, Object>> drivers) {
//
//        int clatitude = (int) message.get("latitude");
//        int clongitude = (int) message.get("longitude");
//        String genderPrefer = (String) (message.get("gender_preference"));
//        double maxScore = 0;
//        int matchedDriverId = 0;
//
//        while (drivers.hasNext()) {
//            Entry<String, Map<String, Object>> driver = drivers.next();
//            int driverId = Integer.valueOf(driver.getKey().split(":")[1]);
//            Map<String, Object> driverMsg = driver.getValue();
//            // Get parameters
//            int dLatitude = (int) driverMsg.get("latitude");
//            int dLongitude = (int) driverMsg.get("longitude");
//            String gender = (String) (driverMsg.get("gender"));
//            double rating = (double) driverMsg.get("rating");
//            int salary = (int) driverMsg.get("salary");
//            // Calculate scores
//            double distance = getDistance(clatitude, clongitude, dLatitude, dLongitude);
//            double genderScore = getGenderScore(genderPrefer, gender);
//            double score = getScore(distance, genderScore, rating, salary);
//
//            if (score > maxScore) {
//                maxScore = score;
//                matchedDriverId = driverId;
//            }
//        }
//        return matchedDriverId;
//    }

    private double getScore(double distance, double gender, double rating, double salary) {
        // rating_score = rating / 5.0
        // salary_score = 1 - salary / 100.0
        // match_score = distance_score * 0.4 + gender_score * 0.2 + rating_score * 0.2 + salary_score * 0.2
        return distance * 0.4 + gender * 0.2 + rating / 25.0 + (1 - salary / 100.0) * 0.2;
    }

    //distance_score = sqrt(1 - client_driver_distance)/MAX_DIST
    private double getDistance(int cLatitude, int cLongitude, int dLatitude, int dLongitude) {
        // escape square root calculation
        double cdDistance = Math.pow(cLatitude - dLatitude, 2) + Math.pow(cLongitude - dLongitude, 2);
        return Math.pow(1 - cdDistance, 0.5) / Math.pow(500, 0.5);
    }

    private double getGenderScore(String cGender, String dGender) {
        if (cGender.equals("N") || cGender.equals(dGender)) {
            return 1.0;
        }
        return 0.0;
    }

    private double getSPF(ArrayList<Integer> driverCount, int availableDriver) {
        double total = availableDriver;
        for (double count : driverCount) {
            total += count;
        }
        double avg = total / 5;
        if (avg >= 3.6) {
            return 1.0;
        }
        double spf = 4 * (3.6 - avg) / 0.8 + 1;
        return spf;
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        //this function is called at regular intervals, not required for this project
    }
}
