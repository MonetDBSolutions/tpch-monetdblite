package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.TPCHSetting;
import nl.cwi.monetdb.populate.TPCHPopulater;

class MonetDatabaseSystem extends DatabaseSystem {
    public MonetDatabaseSystem() {
        super("jdbc:monetdb:", "MonetDB", "nl.cwi.monetdb.jdbc.MonetDriver", new TPCHPopulater(), new TPCHSetting());
    }

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
}
