package xyz.sadiulhakim.project1.data;

import org.springframework.batch.item.ItemProcessor;

public class RawToAggregateSensorDataProcessor implements ItemProcessor<DailySensorData, DailyAggregatedSensorData> {
    @Override
    public DailyAggregatedSensorData process(DailySensorData item) throws Exception {
        double min = item.measurements().getFirst();
        double max = min;
        double sum = 0;

        for (double measurement : item.measurements()) {
            min = Math.min(min, measurement);
            max = Math.max(max, measurement);
            sum += measurement;
        }

        double avg = sum / item.measurements().size();

        return new DailyAggregatedSensorData(item.date(), convertToCelsius(min), convertToCelsius(avg), convertToCelsius(max));
    }

    private static double convertToCelsius(double fahT) {
        return (5 * (fahT - 32)) / 9;
    }
}
