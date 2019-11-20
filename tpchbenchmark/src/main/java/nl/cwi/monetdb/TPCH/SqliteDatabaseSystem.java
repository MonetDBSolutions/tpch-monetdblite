package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.TPCHSetting;
import nl.cwi.monetdb.populate.SqlitePopulator;
import nl.cwi.monetdb.populate.TPCHPopulater;

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

	@Override
	public TPCHPopulater getPopulater() {
		return new SqlitePopulator();
	}
}
