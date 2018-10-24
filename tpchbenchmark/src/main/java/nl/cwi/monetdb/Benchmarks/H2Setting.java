package nl.cwi.monetdb.Benchmarks;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.DriverManager;

public class H2Setting extends TPCHSetting {

	@Override
	public void setupQueries(String queryPath) throws Exception {
		for(int i=1; i <= 22; i++) {
			String nextQuery = Paths.get(queryPath, String.format("%02d.sql", i)).toString();
			byte[] encoded = Files.readAllBytes(Paths.get(nextQuery));
			this.tpchQueries[i - 1] = new String(encoded, Charset.forName("UTF-8"));
		}
	}

	@Override
	public void setupConnectionInternal(String databasePath) throws Exception {
		Class.forName("org.h2.Driver");
		this.connection = DriverManager.getConnection("jdbc:h2:" + databasePath,"sa","");
	}
}