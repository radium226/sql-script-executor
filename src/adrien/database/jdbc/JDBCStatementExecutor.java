package adrien.database.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import adrien.database.StatementExecutor;

public class JDBCStatementExecutor implements StatementExecutor {
	
	final private static Logger LOGGER = LoggerFactory.getLogger(JDBCStatementExecutor.class);
	
	private Connection connection; 
	
	protected JDBCStatementExecutor(Connection connection) {
		super();
		
		setConnection(connection);
	}
	
	protected void setConnection(Connection connection) {
		this.connection = connection;
	}
	
	public Connection getConnection() {
		return connection; 
	}
	
	@Override
	public void executeStatement(String sql) throws SQLException {
		Statement statement = null;
        ResultSet resultSet = null;
        Connection connection = getConnection();
        try {
            statement = connection.createStatement();

            LOGGER.info(sql);

            boolean results = statement.execute(sql);

            resultSet = statement.getResultSet();

            if (results && resultSet != null) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    LOGGER.debug(columnName + "\t");
                }
                
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String value = resultSet.getString(i);
                        LOGGER.debug(value + "\t");
                    }
                }
            }

        } finally {
            closeQuietly(statement, resultSet);
        }
    }

    private void closeQuietly(Statement statement, ResultSet resultSet) {
        closeQuietly(resultSet);
        closeQuietly(statement);
    }
		
    private void closeQuietly(Statement statement) {
		if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOGGER.warn("Unable to close statement", e);
            }
        }
	}
    
    private void closeQuietly(ResultSet resultSet) {
		if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOGGER.warn("Unable to close result set", e);
            }
        }
	}
    
	public static JDBCStatementExecutor forConnection(Connection connection) {
		JDBCStatementExecutor jdbcStatementExecutor = new JDBCStatementExecutor(connection);
		return jdbcStatementExecutor;
	}

}
