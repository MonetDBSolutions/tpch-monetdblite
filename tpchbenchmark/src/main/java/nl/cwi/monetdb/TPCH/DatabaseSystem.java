package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.TPCHSetting;
import nl.cwi.monetdb.populate.TPCHPopulater;

public class DatabaseSystem {
    private final String JdbcUrlPrefix;
    private final String prettyName;
    private final TPCHPopulater populater;
    private final TPCHSetting setting;

    public DatabaseSystem(String jdbcUrlPrefix, String prettyName, TPCHPopulater populater, TPCHSetting setting) {
        JdbcUrlPrefix = jdbcUrlPrefix;
        this.prettyName = prettyName.toLowerCase();
        this.populater = populater;
        this.setting = setting;
    }

    public String getJdbcUrlPrefix() {
        return JdbcUrlPrefix;
    }

    public String getPrettyName() {
        return prettyName;
    }

    public TPCHSetting getSetting() {
        return setting;
    }

    public void populate(ConnectInfo connectInfo, String importPath) throws Exception {
        this.populater.populate(connectInfo, importPath);
    }
}
