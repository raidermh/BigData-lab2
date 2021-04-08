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

@AllArgsConstructor
@Slf4j
public class FlightEventCounter {

    // Формат времени логов - н-р, 'Nov 10 13:54:06'
    private static DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendPattern("MMM dd HH:mm:ss")
            .parseDefaulting(YEAR, 2020)
            .toFormatter();

    /**
     * Функция подсчета количества логов разного уровня в час.
     * Парсит строку лога, в т.ч. уровень логирования и час, в который событие было зафиксировано.
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countFlightPerHour(Dataset<String> inputDataset) {
        Dataset<String> words = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());

        Dataset<FlightHour> logLevelHourDataset = words.map(s -> {
            String[] logFields = s.split(",");
            LocalDateTime date = LocalDateTime.parse(logFields[2], formatter);
            return new FlightHour(logFields[3], logFields[4], date.getHour());
            }, Encoders.bean(FlightHour.class))
                .coalesce(1);

        // Группирует по значениям часа
        Dataset<Row> t = logLevelHourDataset.groupBy("hour","source", "destination")
                .count()
                .toDF("hour", "source", "destination", "count")
                // сортируем по времени лога - для красоты
                .sort(functions.asc("hour"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }

}
