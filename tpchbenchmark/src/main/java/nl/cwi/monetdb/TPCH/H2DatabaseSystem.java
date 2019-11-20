package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.TPCHSetting;
import nl.cwi.monetdb.populate.TPCHPopulater;

class H2DatabaseSystem extends DatabaseSystem {
    public H2DatabaseSystem() {
        super("jdbc:h2:", "H2", "org.h2.Driver", new TPCHPopulater(), new TPCHSetting());
    }

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
}
