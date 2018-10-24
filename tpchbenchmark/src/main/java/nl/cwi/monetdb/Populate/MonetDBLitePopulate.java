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

			st.executeUpdate("COPY INTO REGION   FROM '" + Paths.get(dataPath, "region.tbl").toString()   + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO NATION   FROM '" + Paths.get(dataPath, "nation.tbl").toString()   + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO SUPPLIER FROM '" + Paths.get(dataPath, "supplier.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO CUSTOMER FROM '" + Paths.get(dataPath, "customer.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO PART     FROM '" + Paths.get(dataPath, "part.tbl").toString()     + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO PARTSUPP FROM '" + Paths.get(dataPath, "partsupp.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO ORDERS   FROM '" + Paths.get(dataPath, "orders.tbl").toString()   + "' USING DELIMITERS '|', '|\\n';");
			st.executeUpdate("COPY INTO LINEITEM FROM '" + Paths.get(dataPath, "lineitem.tbl").toString() + "' USING DELIMITERS '|', '|\\n';");

			/* Foreign and primary keys are not required for TPC-H queries because they are read only.
			st.executeUpdate("ALTER TABLE REGION   ADD PRIMARY KEY (R_REGIONKEY);");
			st.executeUpdate("ALTER TABLE NATION   ADD PRIMARY KEY (N_NATIONKEY);");
			st.executeUpdate("ALTER TABLE NATION   ADD CONSTRAINT NATION_FK1 FOREIGN KEY (N_REGIONKEY) REFERENCES REGION;");
			st.executeUpdate("ALTER TABLE PART     ADD PRIMARY KEY (P_PARTKEY);");
			st.executeUpdate("ALTER TABLE SUPPLIER ADD PRIMARY KEY (S_SUPPKEY);");
			st.executeUpdate("ALTER TABLE SUPPLIER ADD CONSTRAINT SUPPLIER_FK1 FOREIGN KEY (S_NATIONKEY) REFERENCES NATION;");
			st.executeUpdate("ALTER TABLE PARTSUPP ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);");
			st.executeUpdate("ALTER TABLE CUSTOMER ADD PRIMARY KEY (C_CUSTKEY);");
			st.executeUpdate("ALTER TABLE CUSTOMER ADD CONSTRAINT CUSTOMER_FK1 FOREIGN KEY (C_NATIONKEY) REFERENCES NATION;");
			st.executeUpdate("ALTER TABLE LINEITEM ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);");
			st.executeUpdate("ALTER TABLE ORDERS   ADD PRIMARY KEY (O_ORDERKEY);");
			st.executeUpdate("ALTER TABLE PARTSUPP ADD CONSTRAINT PARTSUPP_FK1 FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER;");
			st.executeUpdate("ALTER TABLE PARTSUPP ADD CONSTRAINT PARTSUPP_FK2 FOREIGN KEY (PS_PARTKEY) REFERENCES PART;");
			st.executeUpdate("ALTER TABLE ORDERS   ADD CONSTRAINT ORDERS_FK1 FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER;");
			st.executeUpdate("ALTER TABLE LINEITEM ADD CONSTRAINT LINEITEM_FK1 FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS;");
			st.executeUpdate("ALTER TABLE LINEITEM ADD CONSTRAINT LINEITEM_FK2 FOREIGN KEY (L_PARTKEY,L_SUPPKEY) REFERENCES PARTSUPP;");*/
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
