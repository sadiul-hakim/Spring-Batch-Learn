package xyz.sadiulhakim.project1.data;

public record DataAnomaly(
        String date,
        AnomalyType type,
        double value
) {
}
