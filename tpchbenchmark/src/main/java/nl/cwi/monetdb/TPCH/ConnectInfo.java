package nl.cwi.monetdb.TPCH;

public class ConnectInfo {
    private String user;
    private String password;
    private String jdbcUrl;

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

        return new ConnectInfo(user, password, jdbcUrl);
    }

    public ConnectInfo(String user, String password, String jdbcUrl) {
        this.user = user;
        this.password = password;
        this.jdbcUrl = jdbcUrl;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
}
