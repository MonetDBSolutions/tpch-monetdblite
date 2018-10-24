package nl.cwi.monetdb.Benchmarks;

import org.openjdk.jmh.infra.Blackhole;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public abstract class TPCHSetting {

	String[] tpchQueries = new String[22];
	Connection connection;
	private Statement statement;

	abstract void setupQueries(String queryPath) throws Exception;

	abstract void setupConnectionInternal(String databasePath) throws Exception;

	public void setupConnection(String queryPath) throws Exception {
		this.setupConnectionInternal(queryPath);
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
		}
		rs.close();
	}

	public void tpch02(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[1]);
		while (rs.next()) {
			blackhole.consume(rs.getBigDecimal(1));
		}
		rs.close();
	}

	public void tpch03(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[2]);
		while (rs.next()) {
			blackhole.consume(rs.getInt(1));
		}
		rs.close();
	}

	public void tpch04(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[3]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch05(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[4]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch06(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[5]);
		while (rs.next()) {
			blackhole.consume(rs.getBigDecimal(1));
		}
		rs.close();
	}

	public void tpch07(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[6]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch08(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[7]);
		while (rs.next()) {
			blackhole.consume(rs.getInt(1));
		}
		rs.close();
	}

	public void tpch09(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[8]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch10(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[9]);
		while (rs.next()) {
			blackhole.consume(rs.getInt(1));
		}
		rs.close();
	}

	public void tpch11(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[10]);
		while (rs.next()) {
			blackhole.consume(rs.getInt(1));
		}
		rs.close();
	}

	public void tpch12(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[11]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch13(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[12]);
		while (rs.next()) {
			blackhole.consume(rs.getInt(1));
		}
		rs.close();
	}

	public void tpch14(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[13]);
		while (rs.next()) {
			blackhole.consume(rs.getDouble(1));
		}
		rs.close();
	}

	public void tpch15(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[14]);
		while (rs.next()) {
			blackhole.consume(rs.getInt(1));
		}
		rs.close();
	}

	public void tpch16(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[15]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch17(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[16]);
		while (rs.next()) {
			blackhole.consume(rs.getDouble(1));
		}
		rs.close();
	}

	public void tpch18(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[17]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch19(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[18]);
		while (rs.next()) {
			blackhole.consume(rs.getDouble(1));
		}
		rs.close();
	}

	public void tpch20(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[19]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch21(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[20]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}

	public void tpch22(Blackhole blackhole) throws Exception {
		ResultSet rs = this.statement.executeQuery(this.tpchQueries[21]);
		while (rs.next()) {
			blackhole.consume(rs.getString(1));
		}
		rs.close();
	}
}
