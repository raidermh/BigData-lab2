package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Класс FlightHour задает конструктор для парсинга логов
 *
 * @author Mikhail Khrychev
 * @version  1.0.0
 * @since 09.04.2021
 */

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
