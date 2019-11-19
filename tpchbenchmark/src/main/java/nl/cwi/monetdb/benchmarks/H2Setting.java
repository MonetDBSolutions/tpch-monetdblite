package nl.cwi.monetdb.benchmarks;

import java.nio.file.Paths;
import java.sql.DriverManager;

public class H2Setting extends TPCHSetting {

	@Override
	public void setupConnectionInternal(String databasePath) throws Exception {
		Class.forName("org.h2.Driver");
		databasePath = Paths.get(databasePath, "database").toString();
		this.connection = DriverManager.getConnection("jdbc:h2:" + databasePath,"sa","");
	}
}
