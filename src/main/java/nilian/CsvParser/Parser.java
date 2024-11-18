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
        int expectedCommas = Main.csvColumnTypes.getJavaTypes().size() - 1;

        for(int i = 0; i < input.size(); i++) {
            String rawString = input.get(i);
            if (rawString.chars().filter(c -> c == ',').count() == expectedCommas) {
                String[] stringFields = rawString.split(",");
                parsedCsv.add(new CsvRow(Main.csvColumnTypes, stringFields));
            } else {
                badCsv.add(rawString);
            }
        }

        return new Tuple2<>(parsedCsv, badCsv);
    }
}
