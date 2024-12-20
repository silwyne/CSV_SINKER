package nilian.CsvParser.Sink;

import nilian.CsvParser.Tuple.DataType;
import nilian.CsvParser.data.CsvRow;
import nilian.CsvParser.data.Field;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

public class JdbcMaker {

    public static JdbcStatement<CsvRow> getStatement() {
        return (preparedStatement, data) -> {
            List<Field> fields = data.getFields();
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                int position = i + 1;

                if (field.getData() != null) {
                    if(field.getData().isEmpty()) {
                        setNullValue(preparedStatement, position, field.getJavaType());
                    } else {
                        setNonNullValue(preparedStatement, position, field);
                    }
                } else {
                    setNonNullValue(preparedStatement, position, field);
                }
            }
        };
    }

    private static void setNullValue(PreparedStatement preparedStatement, int position, DataType type) throws SQLException {
        switch (type) {
            case String:
                preparedStatement.setNull(position, Types.VARCHAR);
                break;
            case Integer:
                preparedStatement.setNull(position, Types.INTEGER);
                break;
            case Long:
                preparedStatement.setNull(position, Types.BIGINT);
                break;
            case Double:
                preparedStatement.setNull(position, Types.DOUBLE);
                break;
            case Timestamp:
                preparedStatement.setNull(position, Types.TIMESTAMP);
                break;
            case Boolean:
                preparedStatement.setNull(position, Types.BOOLEAN);
                break;
            case INET:
                preparedStatement.setNull(position, Types.OTHER);
                break;
            // Add other types as needed
        }
    }

    private static void setNonNullValue(PreparedStatement preparedStatement, int position, Field field) throws SQLException {
        switch (field.getJavaType()) {
            case String:
                preparedStatement.setString(position, field.getData());
                break;
            case Integer:
                preparedStatement.setInt(position, (int) field.getParsedData());
                break;
            case Long:
                preparedStatement.setLong(position, (long) field.getParsedData());
                break;
            case Double:
                preparedStatement.setDouble(position, (double) field.getParsedData());
                break;
            case Timestamp:
                preparedStatement.setTimestamp(position, (Timestamp) field.getParsedData());
                break;
            case Boolean:
                preparedStatement.setBoolean(position, (Boolean) field.getParsedData());
                break;
            case INET:
                preparedStatement.setObject(position, field.getParsedData(), Types.OTHER);
                break;
            // Add other types as needed
        }
    }
}
