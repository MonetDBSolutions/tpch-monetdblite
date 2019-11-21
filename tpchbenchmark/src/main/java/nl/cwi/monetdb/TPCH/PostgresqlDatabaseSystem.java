package nl.cwi.monetdb.TPCH;

public class PostgresqlDatabaseSystem extends DatabaseSystem {
	public PostgresqlDatabaseSystem() {
		super("jdbc:postgresql:", "PostgreSQL", "org.postgresql.Driver");
	}
}
