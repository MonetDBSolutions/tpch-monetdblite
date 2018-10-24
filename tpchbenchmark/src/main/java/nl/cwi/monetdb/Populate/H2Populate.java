package nl.cwi.monetdb.Populate;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class H2Populate extends TPCHPopulate {

	@Override
	void populateInside(String databasePath, String dataPath) throws Exception {
		Class.forName("org.h2.Driver");
		databasePath = Paths.get(databasePath, "database").toString();
		Connection con = DriverManager.getConnection("jdbc:h2:" + databasePath, "sa","");
		Exception upsme = null;

		con.setAutoCommit(false);
		Statement st = con.createStatement();
		System.out.println("Starting to import into " + databasePath);
		try {
			st.executeUpdate("CREATE TABLE NATION ( N_NATIONKEY  INTEGER NOT NULL," +
					"                      N_NAME       CHAR(25) NOT NULL," +
					"                      N_REGIONKEY  INTEGER NOT NULL," +
					"                      N_COMMENT    VARCHAR(152));");
			st.executeUpdate("CREATE TABLE REGION ( R_REGIONKEY  INTEGER NOT NULL," +
					"                      R_NAME       CHAR(25) NOT NULL," +
					"                      R_COMMENT    VARCHAR(152));");
			st.executeUpdate("CREATE TABLE PART ( P_PARTKEY     INTEGER NOT NULL," +
					"                    P_NAME        VARCHAR(55) NOT NULL," +
					"                    P_MFGR        CHAR(25) NOT NULL," +
					"                    P_BRAND       CHAR(10) NOT NULL," +
					"                    P_TYPE        VARCHAR(25) NOT NULL," +
					"                    P_SIZE        INTEGER NOT NULL," +
					"                    P_CONTAINER   CHAR(10) NOT NULL," +
					"                    P_RETAILPRICE DECIMAL(15,2) NOT NULL," +
					"                    P_COMMENT     VARCHAR(23) NOT NULL);");
			st.executeUpdate("CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL," +
					"                        S_NAME        CHAR(25) NOT NULL," +
					"                        S_ADDRESS     VARCHAR(40) NOT NULL," +
					"                        S_NATIONKEY   INTEGER NOT NULL," +
					"                        S_PHONE       CHAR(15) NOT NULL," +
					"                        S_ACCTBAL     DECIMAL(15,2) NOT NULL," +
					"                        S_COMMENT     VARCHAR(101) NOT NULL);");
			st.executeUpdate("CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL," +
					"                        PS_SUPPKEY     INTEGER NOT NULL," +
					"                        PS_AVAILQTY    INTEGER NOT NULL," +
					"                        PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL," +
					"                        PS_COMMENT     VARCHAR(199) NOT NULL);");
			st.executeUpdate("CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL," +
					"                        C_NAME        VARCHAR(25) NOT NULL," +
					"                        C_ADDRESS     VARCHAR(40) NOT NULL," +
					"                        C_NATIONKEY   INTEGER NOT NULL," +
					"                        C_PHONE       CHAR(15) NOT NULL," +
					"                        C_ACCTBAL     DECIMAL(15,2) NOT NULL," +
					"                        C_MKTSEGMENT  CHAR(10) NOT NULL," +
					"                        C_COMMENT     VARCHAR(117) NOT NULL);");
			st.executeUpdate("CREATE TABLE ORDERS ( O_ORDERKEY       INTEGER NOT NULL," +
					"                      O_CUSTKEY        INTEGER NOT NULL," +
					"                      O_ORDERSTATUS    CHAR(1) NOT NULL," +
					"                      O_TOTALPRICE     DECIMAL(15,2) NOT NULL," +
					"                      O_ORDERDATE      DATE NOT NULL," +
					"                      O_ORDERPRIORITY  CHAR(15) NOT NULL," +
					"                      O_CLERK          CHAR(15) NOT NULL," +
					"                      O_SHIPPRIORITY   INTEGER NOT NULL," +
					"                      O_COMMENT        VARCHAR(79) NOT NULL);");
			st.executeUpdate("CREATE TABLE LINEITEM ( L_ORDERKEY      INTEGER NOT NULL," +
					"                        L_PARTKEY       INTEGER NOT NULL," +
					"                        L_SUPPKEY       INTEGER NOT NULL," +
					"                        L_LINENUMBER    INTEGER NOT NULL," +
					"                        L_QUANTITY      DECIMAL(15,2) NOT NULL," +
					"                        L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL," +
					"                        L_DISCOUNT      DECIMAL(15,2) NOT NULL," +
					"                        L_TAX           DECIMAL(15,2) NOT NULL," +
					"                        L_RETURNFLAG    CHAR(1) NOT NULL," +
					"                        L_LINESTATUS    CHAR(1) NOT NULL," +
					"                        L_SHIPDATE      DATE NOT NULL," +
					"                        L_COMMITDATE    DATE NOT NULL," +
					"                        L_RECEIPTDATE   DATE NOT NULL," +
					"                        L_SHIPINSTRUCT  CHAR(25) NOT NULL," +
					"                        L_SHIPMODE      CHAR(10) NOT NULL," +
					"                        L_COMMENT       VARCHAR(44) NOT NULL);");

			st.execute("INSERT INTO REGION   SELECT * FROM CSVREAD('" + Paths.get(dataPath, "region.tbl").toString()   + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO NATION   SELECT * FROM CSVREAD('" + Paths.get(dataPath, "nation.tbl").toString()   + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO SUPPLIER SELECT * FROM CSVREAD('" + Paths.get(dataPath, "supplier.tbl").toString() + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO CUSTOMER SELECT * FROM CSVREAD('" + Paths.get(dataPath, "customer.tbl").toString() + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO PART     SELECT * FROM CSVREAD('" + Paths.get(dataPath, "part.tbl").toString()     + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO PARTSUPP SELECT * FROM CSVREAD('" + Paths.get(dataPath, "partsupp.tbl").toString() + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO ORDERS   SELECT * FROM CSVREAD('" + Paths.get(dataPath, "orders.tbl").toString()   + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");
			st.execute("INSERT INTO LINEITEM SELECT * FROM CSVREAD('" + Paths.get(dataPath, "lineitem.tbl").toString() + "', null, 'charset=UTF-8 fieldSeparator=| lineSeparator=\\n');");

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
