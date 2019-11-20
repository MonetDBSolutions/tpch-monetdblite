package nl.cwi.monetdb.populate;

import nl.cwi.monetdb.TPCH.ConnectInfo;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.stream.Collectors;

public class TPCHPopulater {

	public void populate(ConnectInfo connectInfo, String importPath) throws Exception {
		System.out.println(String.format("Starting to import into %s, user '%s' password '%s'",
				connectInfo.getJdbcUrl(), connectInfo.getUser(), connectInfo.getPassword()));
		try (
				Connection con = connectInfo.connect();
				Statement st = con.createStatement()
		) {
			con.setAutoCommit(false);
			String[] statements = this.initializationStatements(connectInfo.getDatabaseSystem().getPrettyName(), importPath);
			for (String s : statements) {
				st.executeUpdate(s);
			}
			con.commit();
		}
		System.out.println("Import completed");

	}

	String[] initializationStatements(String databaseName, String dataPath) {
		// Surely there's a better way?
		String resourceName = String.format("/schemas/%s/setup.sql", databaseName);
		InputStream schema = this.getClass().getResourceAsStream(resourceName);
		String content = new BufferedReader(new InputStreamReader(schema)).lines().collect(Collectors.joining("\n"));

		String[] statements = content.split(";");
		for (int i = 0; i < statements.length; i++) {
			statements[i] = statements[i].trim().replaceAll("@DATAPATH@", dataPath) + ";";
		}

		return statements;
	}

}
