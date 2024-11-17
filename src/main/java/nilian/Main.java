package nilian;

import nilian.Reader.FileReader;
import nilian.RunTimeLog.MyLogManager;
import nilian.Sink.Batch.PostgresBatchSink;
import nilian.Sink.JdbcStatement;
import nilian.Tuple.JavaType;
import nilian.Tuple.TupleSchema;
import nilian.data.CsvRow;
import nilian.data.Field;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOGGER = MyLogManager.getLogger("Main");
    private static TupleSchema csvColumnTypes;

    public static void main(String[] args) throws SQLException {

        /*
        Reading Configurations from env.csv_sinker file
         */
        Properties initialProps = EnvLoader.loadEnv("env.csv_sinker");
        String CSV_PATH = initialProps.getProperty("CSV_PATH");
        String CSV_TYPES = initialProps.getProperty("CSV_TYPES");
        boolean CSV_IGNORE_FIRST_LINE = Boolean.parseBoolean(initialProps.getProperty("CSV_IGNORE_FIRST_LINE"));
        String POSTGRES_URL = initialProps.getProperty("POSTGRES_URL");
        String POSTGRES_TABLE = initialProps.getProperty("POSTGRES_TABLE");
        String POSTGRES_USER = initialProps.getProperty("POSTGRES_USER");
        String POSTGRES_PASS = initialProps.getProperty("POSTGRES_PASS");
        String TABLE_COLUMNS = initialProps.getProperty("TABLE_COLUMNS");


        /*
        With logging the configuration
         user makes sure that there are proper Configurations set in program
         */
        LOGGER.info("CSV_PATH          "+CSV_PATH);
        LOGGER.info("CSV_TYPES         "+CSV_TYPES);
        LOGGER.info("IGNORE_FIRST_LINE "+CSV_IGNORE_FIRST_LINE);
        LOGGER.info("POSTGRES_URL      "+POSTGRES_URL);
        LOGGER.info("POSTGRES_TABLE    "+POSTGRES_TABLE);
        LOGGER.info("POSTGRES_USER     "+POSTGRES_USER);
        LOGGER.info("POSTGRES_PASS     "+POSTGRES_PASS);
        LOGGER.info("TABLE_COLUMNS     "+TABLE_COLUMNS);

        /*
        Getting tuple Types from what user said it is
         */
        csvColumnTypes = TupleSchema.parseTuple(CSV_TYPES);
        LOGGER.fine("YOUR CSV DATA TYPES : "+csvColumnTypes);

        /*
        Reading Csv File
         */
        LOGGER.info("READING CSV FILE");
        List<String>
                rawStringFile = FileReader.read(CSV_PATH);
        if(CSV_IGNORE_FIRST_LINE) {
            rawStringFile.remove(0);
        }

        /*
        Casting Raw String into Csv Data
         */
        LOGGER.info("PARSING STRINGS INTO TUPLES");
        List<CsvRow> parsedCsv = new ArrayList<>();
        List<String> badCsv = new ArrayList<>();

        for(String rawString: rawStringFile) {
            String[] stringFields = rawString.split(",");
            if(stringFields.length == csvColumnTypes.getJavaTypes().size()) {
                parsedCsv.add(new CsvRow(csvColumnTypes, stringFields));
            } else {
                badCsv.add(rawString);
                LOGGER.warning("CSV ROW WITH COMMA IN FIELDS "+rawString);
            }
        }

        LOGGER.info("MAKING JdbcStatement");
        JdbcStatement<CsvRow> jdbcStatement = new JdbcStatement<CsvRow>() {
            @Override
            public void setSen(PreparedStatement preparedStatement, CsvRow data) throws SQLException {
                int size = data.getFields().size();
                List<Field> fields = data.getFields();

                for(int i = 0 ; i < size; i++){
                    // getting the prepared position
                    int position = i + 1;

                    // if string
                    if(fields.get(i).getJavaType().equals(JavaType.String)) {
                        if(fields.get(i).getData().isEmpty()) {
                            preparedStatement.setNull(position, Types.VARCHAR);
                        }
                        preparedStatement.setString(position, fields.get(i).getData());
                    }

                    // if integer
                    if(fields.get(i).getJavaType().equals(JavaType.Integer)) {
                        if(fields.get(i).getData().isEmpty()) {
                            preparedStatement.setNull(position, Types.INTEGER);
                        }
                        preparedStatement.setInt(position, (int) fields.get(i).getParsedData());
                    }

                    // if long
                    if(fields.get(i).getJavaType().equals(JavaType.Long)) {
                        if(fields.get(i).getData().isEmpty()) {
                            preparedStatement.setNull(position, Types.BIGINT);
                        }
                        preparedStatement.setLong(position, (long) fields.get(i).getParsedData());
                    }

                    // if Double
                    if(fields.get(i).getJavaType().equals(JavaType.Long)) {
                        if(fields.get(i).getData().isEmpty()) {
                            preparedStatement.setNull(position, Types.DOUBLE);
                        }
                        preparedStatement.setDouble(position, (double) fields.get(i).getParsedData());
                    }
                }
            }
        };

        /*
        Getting Sinker Object
         */
        PostgresBatchSink<CsvRow> sinker = new PostgresBatchSink<CsvRow>(
                POSTGRES_URL,
                POSTGRES_PASS,
                POSTGRES_USER,
                POSTGRES_TABLE,
                TABLE_COLUMNS,
                jdbcStatement
        );

        LOGGER.info("SINKING OK CSV ROWS IN POSTGRES");
        for(CsvRow csvRow: parsedCsv) {
            sinker.sinkData(csvRow);
        }
    }
}
