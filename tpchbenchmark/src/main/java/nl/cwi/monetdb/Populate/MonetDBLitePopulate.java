package nl.cwi.monetdb.Populate;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MonetDBLitePopulate extends TPCHPopulate {

	@Override
	void populateInside(String databasePath, String dataPath) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection("jdbc:monetdb:embedded:" + databasePath,"monetdb","monetdb");
		Exception upsme = null;

		con.setAutoCommit(false);
		Statement st = con.createStatement();
		System.out.println("Starting to import into " + databasePath);
		try {
			st.executeUpdate("CREATE TABLE nation ( n_nationkey  INTEGER NOT NULL," +
					"                      n_name       CHAR(25) NOT NULL," +
					"                      n_regionkey  INTEGER NOT NULL," +
					"                      n_comment    VARCHAR(152));");
			st.executeUpdate("CREATE TABLE region ( r_regionkey  INTEGER NOT NULL," +
					"                      r_name       CHAR(25) NOT NULL," +
					"                      r_comment    VARCHAR(152));");
			st.executeUpdate("CREATE TABLE part ( p_partkey     INTEGER NOT NULL," +
					"                    p_name        VARCHAR(55) NOT NULL," +
					"                    p_mfgr        CHAR(25) NOT NULL," +
					"                    p_brand       CHAR(10) NOT NULL," +
					"                    p_type        VARCHAR(25) NOT NULL," +
					"                    p_size        INTEGER NOT NULL," +
					"                    p_container   CHAR(10) NOT NULL," +
					"                    p_retailprice DECIMAL(15,2) NOT NULL," +
					"                    p_comment     VARCHAR(23) NOT NULL);");
			st.executeUpdate("CREATE TABLE supplier ( s_suppkey     INTEGER NOT NULL," +
					"                        s_name        CHAR(25) NOT NULL," +
					"                        s_address     VARCHAR(40) NOT NULL," +
					"                        s_nationkey   INTEGER NOT NULL," +
					"                        s_phone       CHAR(15) NOT NULL," +
					"                        s_acctbal     DECIMAL(15,2) NOT NULL," +
					"                        s_comment     VARCHAR(101) NOT NULL);");
			st.executeUpdate("CREATE TABLE partsupp ( ps_partkey     INTEGER NOT NULL," +
					"                        ps_suppkey     INTEGER NOT NULL," +
					"                        ps_availqty    INTEGER NOT NULL," +
					"                        ps_supplycost  DECIMAL(15,2)  NOT NULL," +
					"                        ps_comment     VARCHAR(199) NOT NULL);");
			st.executeUpdate("CREATE TABLE customer ( c_custkey     INTEGER NOT NULL," +
					"                        c_name        VARCHAR(25) NOT NULL," +
					"                        c_address     VARCHAR(40) NOT NULL," +
					"                        c_nationkey   INTEGER NOT NULL," +
					"                        c_phone       CHAR(15) NOT NULL," +
					"                        c_acctbal     DECIMAL(15,2) NOT NULL," +
					"                        c_mktsegment  CHAR(10) NOT NULL," +
					"                        c_comment     VARCHAR(117) NOT NULL);");
			st.executeUpdate("CREATE TABLE orders ( o_orderkey       INTEGER NOT NULL," +
					"                      o_custkey        INTEGER NOT NULL," +
					"                      o_orderstatus    CHAR(1) NOT NULL," +
					"                      o_totalprice     DECIMAL(15,2) NOT NULL," +
					"                      o_orderdate      DATE NOT NULL," +
					"                      o_orderpriority  CHAR(15) NOT NULL," +
					"                      o_clerk          CHAR(15) NOT NULL," +
					"                      o_shippriority   INTEGER NOT NULL," +
					"                      o_comment        VARCHAR(79) NOT NULL);");
			st.executeUpdate("CREATE TABLE lineitem ( l_orderkey      INTEGER NOT NULL," +
					"                        l_partkey       INTEGER NOT NULL," +
					"                        l_suppkey       INTEGER NOT NULL," +
					"                        l_linenumber    INTEGER NOT NULL," +
					"                        l_quantity      DECIMAL(15,2) NOT NULL," +
					"                        l_extendedprice DECIMAL(15,2) NOT NULL," +
					"                        l_discount      DECIMAL(15,2) NOT NULL," +
					"                        l_tax           DECIMAL(15,2) NOT NULL," +
					"                        l_returnflag    CHAR(1) NOT NULL," +
					"                        l_linestatus    CHAR(1) NOT NULL," +
					"                        l_shipdate      DATE NOT NULL," +
					"                        l_commitdate    DATE NOT NULL," +
					"                        l_receiptdate   DATE NOT NULL," +
					"                        l_shipinstruct  CHAR(25) NOT NULL," +
					"                        l_shipmode      CHAR(10) NOT NULL," +
					"                        l_comment       VARCHAR(44) NOT NULL);");

			st.executeUpdate("COPY INTO region   FROM '" + Paths.get(dataPath, "region.tbl").toString()   + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO nation   FROM '" + Paths.get(dataPath, "nation.tbl").toString()   + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO supplier FROM '" + Paths.get(dataPath, "supplier.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO customer FROM '" + Paths.get(dataPath, "customer.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO part     FROM '" + Paths.get(dataPath, "part.tbl").toString()     + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO partsupp FROM '" + Paths.get(dataPath, "partsupp.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO orders   FROM '" + Paths.get(dataPath, "orders.tbl").toString()   + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO lineitem FROM '" + Paths.get(dataPath, "lineitem.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");

			/* Foreign and primary keys are not required for TPC-H queries because they are read only.
			st.executeUpdate("ALTER TABLE region   ADD PRIMARY KEY (r_regionkey);");
			st.executeUpdate("ALTER TABLE nation   ADD PRIMARY KEY (n_nationkey);");
			st.executeUpdate("ALTER TABLE nation   ADD CONSTRAINT nation_fk1 FOREIGN KEY (n_regionkey) REFERENCES region;");
			st.executeUpdate("ALTER TABLE part     ADD PRIMARY KEY (p_partkey);");
			st.executeUpdate("ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);");
			st.executeUpdate("ALTER TABLE supplier ADD CONSTRAINT supplier_fk1 FOREIGN KEY (s_nationkey) REFERENCES nation;");
			st.executeUpdate("ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey,ps_suppkey);");
			st.executeUpdate("ALTER TABLE customer ADD PRIMARY KEY (c_custkey);");
			st.executeUpdate("ALTER TABLE customer ADD CONSTRAINT customer_fk1 FOREIGN KEY (c_nationkey) REFERENCES nation;");
			st.executeUpdate("ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey,l_linenumber);");
			st.executeUpdate("ALTER TABLE orders   ADD PRIMARY KEY (o_orderkey);");
			st.executeUpdate("ALTER TABLE partsupp ADD CONSTRAINT partsupp_fk1 FOREIGN KEY (ps_suppkey) REFERENCES supplier;");
			st.executeUpdate("ALTER TABLE partsupp ADD CONSTRAINT partsupp_fk2 FOREIGN KEY (ps_partkey) REFERENCES part;");
			st.executeUpdate("ALTER TABLE orders   ADD CONSTRAINT orders_fk1 FOREIGN KEY (o_custkey) REFERENCES customer;");
			st.executeUpdate("ALTER TABLE lineitem ADD CONSTRAINT lineitem_fk1 FOREIGN KEY (l_orderkey) REFERENCES orders;");
			st.executeUpdate("ALTER TABLE lineitem ADD CONSTRAINT lineitem_fk2 FOREIGN KEY (l_partkey,l_suppkey) REFERENCES partsupp;");*/

			con.commit();
			st.close();
		} catch (Exception ex) {
			upsme = ex;
		}
		try {
			con.close();
		} catch (Exception ex) {
			if (upsme == null)
				upsme = ex;
		}
		if (upsme != null)
			throw upsme;
	}
}
