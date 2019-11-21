package nl.cwi.monetdb.TPCH;

import java.util.ArrayList;

public class DatabaseSystemResolver {

    private static DatabaseSystem[] databaseSystems = new DatabaseSystem[]{
            new MonetDatabaseSystem(),
            new SqliteDatabaseSystem(),
            new PostgresqlDatabaseSystem(),
            new H2DatabaseSystem(),
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


    public static ArrayList<String> getPrefixes() {
        ArrayList<String> prefixes = new ArrayList<>(databaseSystems.length);
        for (DatabaseSystem db: databaseSystems) {
            prefixes.add(db.getJdbcUrlPrefix());
        }
        return prefixes;
    }
}
