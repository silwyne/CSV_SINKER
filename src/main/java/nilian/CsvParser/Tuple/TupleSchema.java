package nilian.CsvParser.Tuple;

import java.util.ArrayList;
import java.util.List;

public class TupleSchema {
    private final List<DataType> dataTypes;

    public TupleSchema(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public static TupleSchema parseTuple(String input) {
        String[] types = input.split(",");
        List<DataType> theTupleDataTypes = new ArrayList<>();
        for(String singleType: types) {
            theTupleDataTypes.add(DataType.valueOf(singleType.strip()));
        }
        return new TupleSchema(theTupleDataTypes);
    }

    @Override
    public String toString() {
        return dataTypes.toString();
    }


    public List<DataType> getJavaTypes() {
        return dataTypes;
    }
}
