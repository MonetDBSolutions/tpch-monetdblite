package nl.cwi.monetdb.Benchmarks;

import java.sql.DriverManager;

public class MonetDBLiteJavaSetting extends TPCHSetting {

	@Override
	public void setupConnectionInternal(String databasePath) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		this.connection = DriverManager.getConnection("jdbc:monetdb:embedded:" + databasePath,"monetdb","monetdb");
	}
}
