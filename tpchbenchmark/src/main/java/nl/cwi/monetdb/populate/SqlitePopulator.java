package nl.cwi.monetdb.populate;

import nl.cwi.monetdb.TPCH.ConnectInfo;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.stream.Collectors;

public class SqlitePopulator extends TPCHPopulater {
	@Override
	protected void innerPopulate(ConnectInfo connectInfo, String importPath, Connection conn) throws SQLException, IOException {
		String[] statements = this.readSqlFromResource(connectInfo.getDatabaseSystem().getPrettyName(), importPath);
		try (Statement st = conn.createStatement()) {
			for (String s : statements) {
				s = s.trim();
//				System.out.println("- " + s);
				if (s.startsWith("@COPY ")) {
					copyData(conn, s.substring(6).trim(), importPath);
				} else {
					st.executeUpdate(s);
				}
			}
		}
	}

	private void copyData(Connection conn, String tableName, String importPath) throws SQLException, IOException {
		// Figure out the column types
		final int columnCount;
		final boolean canBatch = conn.getMetaData().supportsBatchUpdates();
		try (Statement s = conn.createStatement()) {
			ResultSet rs = s.executeQuery("SELECT * FROM " + tableName + " WHERE FALSE");
			ResultSetMetaData md = rs.getMetaData();
			columnCount = md.getColumnCount();
		}

		String update = "INSERT INTO " + tableName + " VALUES (";
		String comma = "";
		for (int i = 0; i < columnCount; i++) {
			update += comma + "?";
			comma = ",";
		}
		update += ");";

		String path = Paths.get(importPath, tableName + ".tbl").toString();
		int count = 0;
		try (
				PreparedStatement s = conn.prepareStatement(update);
				BufferedReader br = new BufferedReader(new FileReader(path));
		) {
			String line;
			int batched = 0;
			while (null != (line = br.readLine())) {
				String[] cells = line.split("\\|");
				for (int i = 0; i < cells.length; i++) {
					s.setString(i + 1, cells[i]);
				}
				if (canBatch) {
					s.addBatch();
					batched++;
					if (batched == 1000) {
						s.executeBatch();
						batched = 0;
					}
				} else {
					s.executeUpdate();
				}
				count++;
			}
			if (batched > 0) {
				s.executeBatch();
			}
		} finally {
			System.out.println(String.format("Wrote %d rows to %s", count, tableName));
		}
	}

	String[] sdfsd(String databaseName, String dataPath) {
		// Surely there's a better way?
		String resourceName = String.format("/schemas/%s/setup.sql", databaseName);
		InputStream schema = this.getClass().getResourceAsStream(resourceName);
		if (schema == null) {
			throw new IllegalArgumentException("Could not find " + resourceName + " on the classpath");
		}
		String content = new BufferedReader(new InputStreamReader(schema)).lines().collect(Collectors.joining("\n"));

		return content.split(";");
	}


}
