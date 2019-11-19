package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.H2Setting;
import nl.cwi.monetdb.benchmarks.MonetDBLiteJavaSetting;
import nl.cwi.monetdb.populate.H2Populater;
import nl.cwi.monetdb.populate.MonetDBLitePopulater;

public class DatabaseSystemResolver {

    private static DatabaseSystem[] databaseSystems = new DatabaseSystem[]{
            new DatabaseSystem("jdbc:h2:", "H2", new H2Populater(), new H2Setting()),
            new DatabaseSystem("jdbc:monetdb:", "MonetDB", new MonetDBLitePopulater(), new MonetDBLiteJavaSetting()),
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
