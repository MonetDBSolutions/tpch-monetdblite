package nl.cwi.monetdb.TPCH;

public class DatabaseSystemResolver {

    private static DatabaseSystem[] databaseSystems = new DatabaseSystem[]{
            new H2DatabaseSystem(),
            new MonetDatabaseSystem(),
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
