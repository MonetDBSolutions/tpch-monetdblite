package nl.cwi.monetdb.populate;

import nl.cwi.monetdb.TPCH.ConnectInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class TPCHPopulater {

	public void populate(ConnectInfo connectInfo, String importPath) throws Exception {
		System.out.println(String.format("Starting to import into %s, user '%s' password '%s'",
				connectInfo.getJdbcUrl(), connectInfo.getUser(), connectInfo.getPassword()));
		try (
				Connection con = connectInfo.connect();
		) {
			con.setAutoCommit(false);
			innerPopulate(connectInfo, importPath, con);
			con.commit();
		}
		System.out.println("Import completed");
	}

	protected void innerPopulate(ConnectInfo connectInfo, String importPath, Connection conn) throws SQLException, IOException {
		String[] statements = this.readSqlFromResource(connectInfo.getDatabaseSystem().getPrettyName(), importPath);
		try (Statement st = conn.createStatement()) {
			for (String s : statements) {
				String fixed = s.replaceAll("@DATAPATH@", importPath) + ";";
				st.executeUpdate(fixed);
			}
		}
	}

	String[] readSqlFromResource(String databaseName, String dataPath) {
		// Surely there's a better way?
		String resourceName = String.format("/sql/%s/setup.sql", databaseName);
		InputStream schema = this.getClass().getResourceAsStream(resourceName);
		if (schema == null) {
			throw new IllegalArgumentException("Could not find " + resourceName + " on the classpath");
		}
		String content = new BufferedReader(new InputStreamReader(schema)).lines().collect(Collectors.joining("\n"));

		return content.split(";");
	}

}
