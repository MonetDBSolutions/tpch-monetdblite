package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.Benchmarks.H2Setting;
import nl.cwi.monetdb.Benchmarks.MonetDBLiteJavaSetting;
import nl.cwi.monetdb.Benchmarks.TPCHBenchmark;
import nl.cwi.monetdb.Benchmarks.TPCHSetting;
import nl.cwi.monetdb.Populate.H2Populate;
import nl.cwi.monetdb.Populate.MonetDBLitePopulate;
import nl.cwi.monetdb.Populate.TPCHPopulate;

public class DatabaseSystemResolver {

    private static class Resolution {
        String fullName;
        Class<? extends TPCHPopulate> populateClass;
        Class<? extends TPCHSetting> settingClass;

        public Resolution(String fullName, Class<? extends TPCHPopulate> populateClass, Class<? extends TPCHSetting> settingClass) {
            this.fullName = fullName.toLowerCase();
            this.populateClass = populateClass;
            this.settingClass = settingClass;
        }

    }

    private static Resolution[] resolutions = new Resolution[]{
            new Resolution("H2", H2Populate.class, H2Setting.class),
            new Resolution("MonetDBLite-Java", MonetDBLitePopulate.class, MonetDBLiteJavaSetting.class),
    };

    public static Class<? extends TPCHPopulate> resolvePopulate(String namePrefix) {
        Resolution res = resolve(namePrefix);
        return res != null ? res.populateClass : null;
    }

    public static Class<? extends TPCHSetting> resolveSetting(String namePrefix) {
        Resolution res = resolve(namePrefix);
        return res != null ? res.settingClass : null;
    }

    private static Resolution resolve(String namePrefix) {
        Resolution result = null;

        String prefix = namePrefix.toLowerCase();
        for (Resolution res : resolutions) {
            if (!res.fullName.startsWith(prefix)) {
                continue;
            }
            if (result != null) {
                return null;
            }
            result = res;
        }

        return result;
    }

}
