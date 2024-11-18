package nilian.CsvParser.data;

import nilian.CsvParser.Tuple.TupleSchema;

import java.util.ArrayList;
import java.util.List;

public class CsvRow {

    private final List<Field> csvFields;

    public CsvRow(TupleSchema tupleSchema, String[] rawString) {
        // get fields
        csvFields = new ArrayList<>();
        int tupleSchemaSize = tupleSchema.getJavaTypes().size();
        for(int i = 0 ; i < tupleSchemaSize; i++) {
            if(rawString.length > i) {
                csvFields.add(new Field(rawString[i], tupleSchema.getJavaTypes().get(i)));
            } else {
                csvFields.add(new Field(null, tupleSchema.getJavaTypes().get(i)));
            }
        }
    }

    public List<Field> getFields() {
        return csvFields;
    }
}
