package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;

import static java.time.temporal.ChronoField.YEAR;

/**
 * Класс FlightEventCounter подготавливает датасеты нужные для расчета
 * кол-во полетов в час из страны назначения в страну прилета
 *
 * @author Mikhail Khrychev
 * @version  1.0.0
 * @since 09.04.2021
 */

@AllArgsConstructor
@Slf4j
public class FlightEventCounter {

    // Форматтер времени логов
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("MMM dd HH:mm:ss")
            .parseDefaulting(YEAR, 2020)
            .toFormatter();

    /**
     * Функция подсчета количества рейсов из страны в страну в час.
     * Парсит строку лога, страну вылета и прилета, час выелта.
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countFlightPerHour(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        Dataset<FlightHour> flightHourDataset = words.map(s -> {
            String[] logFields = s.split(",");
            LocalDateTime date = LocalDateTime.parse(logFields[2], formatter);
            return new FlightHour(logFields[3], logFields[4], date.getHour());
            }, Encoders.bean(FlightHour.class))
                .coalesce(1);

        // Группирует по значениям часа
        Dataset<Row> t = flightHourDataset.groupBy("hour","source", "destination")
                .count()
                .toDF("hour", "source", "destination", "count")
                // сортируем по времени лога
                .sort(functions.asc("hour"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
