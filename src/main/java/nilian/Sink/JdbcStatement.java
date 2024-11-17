package nilian.Sink;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface JdbcStatement<T> {

    void setSen(PreparedStatement preparedStatement, T data) throws SQLException;
}
