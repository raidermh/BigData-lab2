package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class FlightHour {

    // Аэропорт отправления
    private String source;

    // Аэропорт назначения
    private String destination;

    // Час, в который произошло событие
    private int hour;
}
