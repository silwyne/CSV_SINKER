package nilian.CsvParser;

import nilian.CsvParser.Tuple.Tuple2;
import nilian.CsvParser.data.CsvRow;
import nilian.Main;

import java.util.ArrayList;
import java.util.List;

public class Parser {

    public static Tuple2<List<CsvRow>, List<String>> parse(List<String> input) {

        List<CsvRow> parsedCsv = new ArrayList<>();
        List<String> badCsv = new ArrayList<>();
        int expectedColumns = Main.csvColumnTypes.getJavaTypes().size();

        /*
        Parsing Csv lines one by one
         */
        for(int i = 0; i < input.size(); i++) {
            List<String> csvFields = parseCsvLine(input.get(i));
            if(csvFields.size() >= expectedColumns) {
                parsedCsv.add(new CsvRow(Main.csvColumnTypes, csvFields));
            } else {
                badCsv.add(input.get(i));

            }
        }

        return new Tuple2<>(parsedCsv, badCsv);
    }

    private static List<String> parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (c == '\"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(currentField.toString());
                currentField = new StringBuilder();
            } else {
                currentField.append(c);
            }
        }

        fields.add(currentField.toString());
        return fields;
    }
}
