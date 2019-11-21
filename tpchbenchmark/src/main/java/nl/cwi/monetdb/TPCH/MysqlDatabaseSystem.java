package nl.cwi.monetdb.TPCH;

public class MysqlDatabaseSystem extends DatabaseSystem {
	public MysqlDatabaseSystem() {
		super("jdbc:mysql:", "MySQL", "com.mysql.cj.jdbc.Driver");
	}
}
