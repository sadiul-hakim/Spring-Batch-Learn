package xyz.sadiulhakim.project1.data;

import com.thoughtworks.xstream.security.ExplicitTypePermission;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.HashMap;
import java.util.Map;

public class DailyAggregatedSensorData{
        private String date;
        private double min;
        private double avg;
        private double max;

    public DailyAggregatedSensorData(String date, double min, double avg, double max) {
        this.date = date;
        this.min = min;
        this.avg = avg;
        this.max = max;
    }

    public static final String ITEM_ROOT_ELEMENT_NAME = "daily-data";

    public static XStreamMarshaller getMarshaller() {
        var marshaller = new XStreamMarshaller();

        Map<String, Class> aliases = new HashMap<>();
        aliases.put(ITEM_ROOT_ELEMENT_NAME, DailyAggregatedSensorData.class);
        aliases.put("date", String.class);
        aliases.put("min", Double.class);
        aliases.put("avg", Double.class);
        aliases.put("max", Double.class);

        ExplicitTypePermission typePermission = new ExplicitTypePermission(new Class[] {DailyAggregatedSensorData.class});

        marshaller.setAliases(aliases);
        marshaller.setTypePermissions(typePermission);

        return marshaller;
    }

    public String date() {
        return date;
    }

    public double min() {
        return min;
    }

    public double avg() {
        return avg;
    }

    public double max() {
        return max;
    }
}
