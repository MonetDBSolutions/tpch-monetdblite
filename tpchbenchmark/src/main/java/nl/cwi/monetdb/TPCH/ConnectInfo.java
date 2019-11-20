package nl.cwi.monetdb.TPCH;

public class ConnectInfo {
    private final String user;
    private final String password;
    private final String jdbcUrl;
    private final DatabaseSystem databaseSystem;

    public static ConnectInfo parse(String connectString) {
        final String user;
        final String password;
        final String jdbcUrl;

        if (connectString.startsWith("jdbc:")) {
            user = null;
            password = null;
            jdbcUrl = connectString;
        } else if (connectString.contains("@jdbc:")) {
            int atIdx = connectString.indexOf("@jdbc:");
            jdbcUrl = connectString.substring(atIdx + 1);
            int slashIdx = connectString.indexOf("/");
            if (slashIdx < atIdx) {
                user = connectString.substring(0, slashIdx);
                password = connectString.substring(slashIdx + 1, atIdx);
            } else {
                user = connectString.substring(0, atIdx);
                password = null;
            }
        } else {
            return null;
        }

        DatabaseSystem sys = DatabaseSystemResolver.resolve(jdbcUrl);
        if (sys == null) {
            return null;
        }

        return new ConnectInfo(sys.fillInUser(user, password), sys.fillInPassword(user, password), jdbcUrl, sys);
    }

    public ConnectInfo(String user, String password, String jdbcUrl, DatabaseSystem databaseSystem) {
        this.user = user;
        this.password = password;
        this.jdbcUrl = jdbcUrl;
        this.databaseSystem = databaseSystem;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public DatabaseSystem getDatabaseSystem() {
        return databaseSystem;
    }

    public String getDriverClass() {
        return this.getDatabaseSystem().getDriverClass();
    }
}
