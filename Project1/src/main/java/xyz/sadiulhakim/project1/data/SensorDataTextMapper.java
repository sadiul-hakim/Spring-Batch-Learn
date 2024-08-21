package xyz.sadiulhakim.project1.data;

import org.springframework.batch.item.file.LineMapper;

import java.util.Arrays;

public class SensorDataTextMapper implements LineMapper<DailySensorData> {

    @Override
    public DailySensorData mapLine(String line, int lineNumber) throws Exception {
        var data = line.split(":");
        return new DailySensorData(data[0],
                Arrays.stream(data[1].split(","))
                        .map(Double::parseDouble)
                        .toList()
        );
    }
}
