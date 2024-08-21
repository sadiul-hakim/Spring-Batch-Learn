package xyz.sadiulhakim.project1.data;

import org.springframework.batch.item.ItemProcessor;

public class SensorDataAnomalyProcessor implements ItemProcessor<DailyAggregatedSensorData, DataAnomaly> {

    private static final double THRESHOLD = 0.9;

    @Override
    public DataAnomaly process(DailyAggregatedSensorData item) {
        if ((item.min() / item.avg()) < THRESHOLD) {
            return new DataAnomaly(item.date(), AnomalyType.MINIMUM, item.min());
        } else if ((item.avg() / item.max()) < THRESHOLD) {
            return new DataAnomaly(item.date(), AnomalyType.MAXIMUM, item.max());
        } else {
            return null;
        }
    }
}
