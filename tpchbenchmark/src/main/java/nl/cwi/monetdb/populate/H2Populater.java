package nl.cwi.monetdb.populate;

import nl.cwi.monetdb.TPCH.ConnectInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class H2Populater extends TPCHPopulater {

    @Override
    public void populate(ConnectInfo connectInfo, String importPath) throws Exception {
        Class.forName("org.h2.Driver");
        String user = connectInfo.getUser();
        String password = connectInfo.getPassword();
        String jdbcUrl = connectInfo.getJdbcUrl();

        if (user == null && password == null) {
            user = "sa";
            password = "";
        }
        if (user == null && password == null) {
            user = "sa";
            password = "monetdb";
        }

        System.out.println(String.format("Starting to import into %s, user %s password %s", jdbcUrl, user, password));
        try (
                Connection con = DriverManager.getConnection(jdbcUrl, user, password);
                Statement st = con.createStatement()
        ) {
            con.setAutoCommit(false);
            String[] statements = this.initializationStatements("H2", importPath);
            for (String s : statements) {
                st.executeUpdate(s);
            }
            con.commit();
        }
        System.out.println("Import completed");

    }
}
