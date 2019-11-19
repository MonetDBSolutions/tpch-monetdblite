package nl.cwi.monetdb.populate;

import nl.cwi.monetdb.TPCH.ConnectInfo;

import java.io.*;
import java.util.stream.Collectors;

public abstract class TPCHPopulater {

	public abstract void populate(ConnectInfo connectInfo, String importPath) throws Exception;

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
