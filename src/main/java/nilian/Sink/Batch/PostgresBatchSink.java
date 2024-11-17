package nilian.Sink.Batch;

import nilian.Sink.JdbcStatement;
import nilian.Tuple.Tuple13;

import java.sql.*;

public class PostgresBatchSink<T>{

    private final String columns;
    private final PreparedStatement preparedStatement ;
    private final JdbcStatement<T> jdbcStatement;

    public PostgresBatchSink(String url,
                             String pass,
                             String user,
                             String tableName,
                             String columns,
                             JdbcStatement<T> jdbcStatement) throws SQLException {
        this.jdbcStatement = jdbcStatement;
        this.columns = columns ;
        String sql = makeSql(tableName);
        Connection connection = DriverManager.getConnection(url, user, pass);
        preparedStatement = connection.prepareStatement(sql);
    }

    public void sinkData(T data) throws SQLException {
        jdbcStatement.setSen(preparedStatement, data);
        preparedStatement.executeUpdate();
    }

    private String makeSql(String tableName) {
        String questionMarks = "?, ".repeat(Math.max(0, columns.split(",").length - 1)) +
                "?";
        return "INSERT INTO "+tableName+" ("+columns+") VALUES ("+ questionMarks +")" ;
    }
}
