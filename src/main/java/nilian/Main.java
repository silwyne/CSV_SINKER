package nilian;

import nilian.CsvParser.Sink.JdbcMaker;
import nilian.HO.EnvLoader;
import nilian.Reader.FileReader;
import nilian.RunTimeLog.MyLogManager;
import nilian.CsvParser.Sink.Batch.PostgresBatchSink;
import nilian.CsvParser.Sink.JdbcStatement;
import nilian.CsvParser.Tuple.TupleSchema;
import nilian.CsvParser.data.CsvRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The Main Class Calls from Other Classes to handle the process
 * This is the Main Class of this project
 *
 * @author seyed mohamad hasan tabatabaei asl
 */
public class Main {

    private static final Logger LOGGER = MyLogManager.getLogger("Main");
    private static TupleSchema csvColumnTypes;
    private static int writtenRows = 0 ;
    public static String TIMESTAMP_PATTERN;
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
        TIMESTAMP_PATTERN = initialProps.getProperty("TIMESTAMP_PATTERN");


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
        LOGGER.info("TIMESTAMP_PATTERN "+TIMESTAMP_PATTERN);

        /*
        Getting tuple Types from what user said it is
         */
        csvColumnTypes = TupleSchema.parseTuple(CSV_TYPES);
        LOGGER.fine("YOUR CSV DATA TYPES WITH "+csvColumnTypes.getJavaTypes().size()+" COLS : "+csvColumnTypes);

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
            }
        }

        LOGGER.info("MAKING JdbcStatement");
        JdbcStatement<CsvRow> jdbcStatement = JdbcMaker.getStatement();

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

        /*
        Sinking Operation
         */
        LOGGER.info("SINKING OK CSV ROWS IN POSTGRES ...");
        for(CsvRow csvRow: parsedCsv) {
            sinker.sinkData(csvRow);
            writtenRows ++ ;
        }
        LOGGER.finest("Done!");
        LOGGER.finest(writtenRows+" sinked into Table");
        LOGGER.warning(badCsv.size()+" bad csv rows");

        if(!badCsv.isEmpty()) {
            LOGGER.warning("You have some csv lines didn't sinked into Postgres table!");
            LOGGER.warning("Here they are");
            for(String badLine: badCsv) {
                System.out.println(badLine);
            }
        }
    }
}
