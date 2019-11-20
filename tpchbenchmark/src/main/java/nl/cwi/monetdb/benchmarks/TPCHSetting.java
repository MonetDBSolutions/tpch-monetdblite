package nl.cwi.monetdb.benchmarks;

import nl.cwi.monetdb.TPCH.ConnectInfo;
import nl.cwi.monetdb.TPCH.DatabaseSystem;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Collectors;

public class TPCHSetting {

	private String[] tpchQueries = new String[22];
	Connection connection;
	private Statement statement;

	public void setupQueries(DatabaseSystem databaseSystem, double scaleFactor) throws Exception {
		for(int i=1; i <= 22; i++) {
			String next = getQuery(databaseSystem, i);

			if(i == 11) {
				String replace = String.format("%f", 0.0001f / scaleFactor);
				next = next.replaceAll("@REPLACE_ME@", replace);
			}
			this.tpchQueries[i - 1] = next;
		}
	}

	String getQuery(DatabaseSystem databaseSystem, int i) {
		// Ugly.  See also TPCHPopulator.initializationStatements
		String resourceName = String.format("/queries/%s/%02d.sql", databaseSystem.getPrettyName(), i);
		InputStream s = this.getClass().getResourceAsStream(resourceName);
		if (s == null) {
			throw new IllegalArgumentException("Could not find resource " + resourceName + " on classpath");
		}
		String content = new BufferedReader(new InputStreamReader(s)).lines().collect(Collectors.joining("\n"));
		return content;
	}

	public void setupConnection(ConnectInfo connectInfo) throws Exception {
		this.connection = connectInfo.connect();
		this.statement = connection.createStatement();
	}

	public void closeConnection() throws Exception {
		this.statement.close();
		this.connection.close();
	}

	public void tpch01(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[0]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
			blackhole.consume(rs.getString(5));
			blackhole.consume(rs.getString(6));
			blackhole.consume(rs.getString(7));
			blackhole.consume(rs.getString(8));
			blackhole.consume(rs.getString(9));
			blackhole.consume(rs.getString(10));
		}
		rs.close();
	}

	public void tpch02(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[1]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
			blackhole.consume(rs.getString(5));
			blackhole.consume(rs.getString(6));
			blackhole.consume(rs.getString(7));
			blackhole.consume(rs.getString(8));
		}
		rs.close();
	}

	public void tpch03(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[2]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
		}
		rs.close();
	}

	public void tpch04(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[3]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch05(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[4]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch06(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[5]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch07(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[6]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
		}
		rs.close();
	}

	public void tpch08(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[7]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch09(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[8]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
		}
		rs.close();
	}

	public void tpch10(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[9]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
			blackhole.consume(rs.getString(5));
			blackhole.consume(rs.getString(6));
			blackhole.consume(rs.getString(7));
			blackhole.consume(rs.getString(8));
		}
		rs.close();
	}

	public void tpch11(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[10]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch12(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[11]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
		}
		rs.close();
	}

	public void tpch13(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[12]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch14(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[13]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch15(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[14]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
			blackhole.consume(rs.getString(5));
		}
		rs.close();
	}

	public void tpch16(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[15]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
		}
		rs.close();
	}

	public void tpch17(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[16]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch18(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[17]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
			blackhole.consume(rs.getString(4));
			blackhole.consume(rs.getString(5));
			blackhole.consume(rs.getString(6));
		}
		rs.close();
	}

	public void tpch19(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[18]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch20(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[19]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch21(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[20]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
		}
		rs.close();
	}

	public void tpch22(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[21]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
			blackhole.consume(rs.getString(2));
			blackhole.consume(rs.getString(3));
		}
		rs.close();
	}
}
