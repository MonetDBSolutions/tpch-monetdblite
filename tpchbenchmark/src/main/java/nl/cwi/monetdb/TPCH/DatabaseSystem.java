package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.TPCHSetting;
import nl.cwi.monetdb.populate.TPCHPopulater;

public abstract class DatabaseSystem {
    private final String JdbcUrlPrefix;
    private final String prettyName;
    private final String driverClass;

    public DatabaseSystem(String jdbcUrlPrefix, String prettyName, String driverClass) {
        JdbcUrlPrefix = jdbcUrlPrefix;
        this.prettyName = prettyName;
        this.driverClass = driverClass;
    }

    public String getJdbcUrlPrefix() {
        return JdbcUrlPrefix;
    }

    public String getPrettyName() {
        return prettyName;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String fillInUser(String user, String password) {
        return user;
    }

    public String fillInPassword(String user, String password) {
        return password;
    }

    public TPCHPopulater getPopulater() {
        return new TPCHPopulater();
    }

    public TPCHSetting getSetting() {
        return new TPCHSetting();
    }

}
