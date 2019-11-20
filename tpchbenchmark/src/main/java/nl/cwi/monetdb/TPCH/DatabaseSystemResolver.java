package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.H2Setting;
import nl.cwi.monetdb.benchmarks.MonetDBLiteJavaSetting;
import nl.cwi.monetdb.populate.H2Populater;
import nl.cwi.monetdb.populate.MonetDBLitePopulater;

public class DatabaseSystemResolver {

    private static DatabaseSystem[] databaseSystems = new DatabaseSystem[]{
            new DatabaseSystem("jdbc:h2:", "H2", new H2Populater(), new H2Setting()) {
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
            new DatabaseSystem("jdbc:monetdb:", "MonetDB", new MonetDBLitePopulater(), new MonetDBLiteJavaSetting()) {
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
