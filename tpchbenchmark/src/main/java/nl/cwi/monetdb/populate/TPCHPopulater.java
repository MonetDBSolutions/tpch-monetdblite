package nl.cwi.monetdb.populate;

import nl.cwi.monetdb.TPCH.ConnectInfo;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Duration;
import java.util.Locale;
import java.util.stream.Collectors;

public class TPCHPopulater {

	public void populate(ConnectInfo connectInfo, String importPath) throws Exception {
		System.out.println(String.format("Starting to import into %s, user '%s' password '%s'",
				connectInfo.getJdbcUrl(), connectInfo.getUser(), connectInfo.getPassword()));

		long t0 = System.nanoTime();
		try (
				Connection con = connectInfo.connect();
		) {
			con.setAutoCommit(false);
			innerPopulate(connectInfo, importPath, con);
			con.commit();
		}
		long t1 = System.nanoTime();
		double d = (t1 - t0) / 1e9;
		System.out.printf("Import completed in %.02fs%n", d);
	}

	protected void innerPopulate(ConnectInfo connectInfo, String importPath, Connection conn) throws SQLException, IOException {
		String[] statements = this.readSqlFromResource(connectInfo.getDatabaseSystem().getPrettyName(), importPath);
		try (Statement st = conn.createStatement()) {
			for (String s : statements) {
				s = s.trim();
				if (s.startsWith("@COPY ")) {
					copyData(conn, s.substring(6), importPath);
				} else {
					String fixed = s.replaceAll("@DATAPATH@", importPath) + ";";
					st.executeUpdate(fixed);
				}
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

	private enum ColumnKind {
		Num,
		Date,
		Text,
	}

	protected void copyData(Connection conn, String tableName, String importPath) throws SQLException, IOException {
		// Figure out the column types
		final int columnCount;
		final boolean canBatch = conn.getMetaData().supportsBatchUpdates();
		ColumnKind[] columnKind;
		try (Statement s = conn.createStatement()) {
			ResultSet rs = s.executeQuery("SELECT * FROM " + tableName + " WHERE FALSE");
			ResultSetMetaData md = rs.getMetaData();
			columnCount = md.getColumnCount();
			columnKind = new ColumnKind[columnCount];
			for (int i = 0; i < columnCount; i++) {
				columnKind[i] = determineColumnKind(md, i + 1);
			}
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
					switch (columnKind[i]) {
						case Num:
							s.setDouble(i + 1, Double.parseDouble(cells[i]));
							break;
						case Date:
							Date d = Date.valueOf(cells[i]);
							s.setDate(i + 1, d);
							break;
						case Text:
							s.setString(i + 1, cells[i]);
							break;
					}
				}
				if (canBatch) {
					s.addBatch();
					batched++;
					if (batched == 1000) {
						s.executeBatch();
						count += batched;
						batched = 0;
					}
				} else {
					s.executeUpdate();
					count++;
				}
			}
			if (batched > 0) {
				s.executeBatch();
				count += batched;
			}
		}
	}

	private ColumnKind determineColumnKind(ResultSetMetaData md, int i) throws SQLException {
		String typeName = md.getColumnTypeName(i).toLowerCase(Locale.US);
		if (typeName.contains("int") || typeName.contains("numeric")) {
			return ColumnKind.Num;
		}
		if (typeName.contains("date")) {
			return ColumnKind.Date;
		}
		return ColumnKind.Text;
	}
}
