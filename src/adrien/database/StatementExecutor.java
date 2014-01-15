package adrien.database;

import java.sql.SQLException;

public interface StatementExecutor {

	void executeStatement(String sql) throws SQLException;
	
}
