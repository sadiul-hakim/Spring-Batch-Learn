package xyz.sadiulhakim.project1.data;

import java.util.List;

public record DailySensorData(
        String date,
        List<Double> measurements
) {
}
