package nilian.CsvParser.data;

import nilian.CsvParser.Tuple.JavaType;

import java.sql.Timestamp;

public class Field {
    private final JavaType javaType;
    private final String data;

    public Field(String data, JavaType javaType) {
        this.data = data;
        this.javaType = javaType;
    }

    public JavaType getJavaType() {
        return javaType;
    }

    public String getData() {
        return data;
    }

    public Object getParsedData(){
        switch (javaType) {

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
                return Timestamp.valueOf(data);
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
