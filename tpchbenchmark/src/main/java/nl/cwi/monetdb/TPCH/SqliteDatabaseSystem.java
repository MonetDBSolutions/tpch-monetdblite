package nl.cwi.monetdb.TPCH;

public class SqliteDatabaseSystem extends DatabaseSystem {
	public SqliteDatabaseSystem() {
		super("jdbc:sqlite:", "SQLite", "org.sqlite.JDBC");
	}

	@Override
	public String fillInUser(String user, String password) {
		return null;
	}

	@Override
	public String fillInPassword(String user, String password) {
		return null;
	}
}
