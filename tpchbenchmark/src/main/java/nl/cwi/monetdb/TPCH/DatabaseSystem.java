package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.TPCHSetting;
import nl.cwi.monetdb.populate.TPCHPopulater;

public abstract class DatabaseSystem {
    private final String JdbcUrlPrefix;
    private final String prettyName;
    private final String driverClass;
    private final TPCHPopulater populater;
    private final TPCHSetting setting;

    public DatabaseSystem(String jdbcUrlPrefix, String prettyName, String driverClass, TPCHPopulater populater, TPCHSetting setting) {
        JdbcUrlPrefix = jdbcUrlPrefix;
        this.prettyName = prettyName;
        this.populater = populater;
        this.setting = setting;
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

    public abstract String fillInUser(String user, String password);

    public abstract String fillInPassword(String user, String password);

    public TPCHSetting getSetting() {
        return setting;
    }

    public void populate(ConnectInfo connectInfo, String importPath) throws Exception {
        this.populater.populate(connectInfo, importPath);
    }
}
