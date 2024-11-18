package nilian.CsvParser.data;

import nilian.CsvParser.Tuple.DataType;
import nilian.Main;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Field {
    private final DataType dataType;
    private final String data;

    public Field(String data, DataType dataType) {
        this.data = data;
        this.dataType = dataType;
    }

    public DataType getJavaType() {
        return dataType;
    }

    public String getData() {
        return data;
    }

    public Object getParsedData(){
        switch (dataType) {

            case String: return data;

            case Integer: {
                if(data.isEmpty()) {
                    return null;
                }
                return Integer.parseInt(data);
            }

            case Double: {
                if(data.isEmpty()) {
                    return null;
                }
                return Double.parseDouble(data);
            }

            case Long: {
                if(data.isEmpty()) {
                    return null;
                }
                return Long.parseLong(data);
            }

            case Timestamp: {
                if(data.isEmpty()) {
                    return null;
                }
                return Timestamp.valueOf(LocalDateTime.parse(data, DateTimeFormatter.ofPattern(Main.TIMESTAMP_PATTERN)));
            }

            case Boolean: {
                if(data.isEmpty()) {
                    return null;
                }
                return Boolean.valueOf(data);
            }
        }
        // if none
        return null;
    }


}
