package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.H2Setting;
import nl.cwi.monetdb.benchmarks.MonetDBLiteJavaSetting;
import nl.cwi.monetdb.populate.TPCHPopulater;

public class DatabaseSystemResolver {

    private static DatabaseSystem[] databaseSystems = new DatabaseSystem[]{
            new DatabaseSystem("jdbc:h2:", "H2", "org.h2.Driver", new TPCHPopulater(), new H2Setting()) {
                @Override
                public String fillInUser(String user, String password) {
                    if (user == null && password == null) {
                        return "sa";
                    }
                    return user;
                }

                @Override
                public String fillInPassword(String user, String password) {
                    if (password == null) {
                        return "";
                    }
                    return password;
                }
            },
            new DatabaseSystem("jdbc:monetdb:", "MonetDB", "nl.cwi.monetdb.jdbc.MonetDriver", new TPCHPopulater(), new MonetDBLiteJavaSetting()) {
                @Override
                public String fillInUser(String user, String password) {
                    if (user == null) {
                        return "monetdb";
                    }
                    return user;
                }

                @Override
                public String fillInPassword(String user, String password) {
                    if (user == null && password == null) {
                        return "monetdb";
                    }
                    return password;
                }
            },
    };

    public static DatabaseSystem resolve(String jdbcUrl) {
        DatabaseSystem result = null;

        for (DatabaseSystem sys : databaseSystems) {
            if (!jdbcUrl.startsWith(sys.getJdbcUrlPrefix())) {
                continue;
            }
            if (result != null) {
                // duplicate
                return null;
            }
            result = sys;
        }

        return result;
    }



}
