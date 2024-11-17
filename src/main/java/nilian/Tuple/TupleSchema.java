package nilian.Tuple;

import java.util.ArrayList;
import java.util.List;

public class TupleSchema {
    private final List<JavaType> javaTypes;

    public TupleSchema(List<JavaType> javaTypes) {
        this.javaTypes = javaTypes;
    }

    public static TupleSchema parseTuple(String input) {
        String[] types = input.split(",");
        List<JavaType> theTupleJavaTypes = new ArrayList<>();
        for(String singleType: types) {
            theTupleJavaTypes.add(JavaType.valueOf(singleType.strip()));
        }
        return new TupleSchema(theTupleJavaTypes);
    }

    @Override
    public String toString() {
        return javaTypes.toString();
    }


    public List<JavaType> getJavaTypes() {
        return javaTypes;
    }
}
