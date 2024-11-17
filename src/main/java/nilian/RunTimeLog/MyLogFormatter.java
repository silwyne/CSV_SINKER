package nilian.RunTimeLog;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class MyLogFormatter extends Formatter
{

    private static final DateTimeFormatter dateFormatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(ZoneId.systemDefault());


    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";

    @Override
    public String format(LogRecord record)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(dateFormatter.format(Instant.ofEpochMilli(record.getMillis())))
                .append(" ");

        // Set color based on log level
        switch (record.getLevel().getName()) {
            case "SEVERE":
                builder.append(ANSI_RED);
                break;
            case "WARNING":
                builder.append(ANSI_YELLOW);
                break;
            case "INFO":
                builder.append(ANSI_GREEN);
                break;
            case "CONFIG":
                builder.append(ANSI_BLUE);
            case "FINE":
                builder.append(ANSI_YELLOW);
            case "FINER":
            case "FINEST":
                builder.append(ANSI_BLUE);
                break;
        }

        builder
                .append("[")
                .append(record.getLevel().getName())
                .append("] ")
                .append(record.getLoggerName())
                .append(" - ")
                .append(record.getSourceMethodName())
                .append(" - ")
                .append(formatMessage(record))
                .append("\n")
                .append(ANSI_RESET);
        return builder.toString();
    }
}
