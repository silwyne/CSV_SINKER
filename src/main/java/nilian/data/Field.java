package nilian.data;

import nilian.Tuple.JavaType;

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
        // if string
        if(javaType.equals(JavaType.String)) {
            return data;
        }

        // if integer
        if(javaType.equals(JavaType.Integer)) {
            if(data.isEmpty()) {
                return null;
            }
            return Integer.parseInt(data);
        }

        // if double
        if(javaType.equals(JavaType.Double)) {
            if(data.isEmpty()) {
                return null;
            }
            return Double.parseDouble(data);
        }

        // if long
        if(javaType.equals(JavaType.Long)) {
            if(data.isEmpty()) {
                return null;
            }
            return Long.parseLong(data);
        }

        // else null
        return null;
    }


}
