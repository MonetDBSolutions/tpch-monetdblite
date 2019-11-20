package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.benchmarks.*;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.TimeUnit;

public class TPCHMain {
    //	private String
    public static void displayError(String message) {
        System.err.println(message);
        System.exit(1);
    }

    private static void displayHelp(int status) {
        String[] text = {
                "Use of JVM TPC-H benchmark tool:",
                "    java -jar tpcbenchmark.jar populate [USER[/PASSWORD]@]JDBC_URL IMPORT_PATH",
                "    java -jar tpcbenchmark.jar evaluate [USER[/PASSWORD]@]JDBC_URL SCALE_FACTOR ",
                "    java -jar tpcbenchmark.jar help",
                "with JDBC_URL one of",
                "    jdbc:h2:...",
                "    jdbc:monetdb:embedded:...",
                "    jdbc:monetdb:...",
                "for example,",
                "    jdbc:h2:~/test,",
                "    sa@jdbc:h2:~/test, ",
                "    monetdb/monetdb@jdbc:monetdb://localhost:50000/test",
                "    jdbc:monetdb:embedded:/tmp/testdb",
                "and IMPORT_PATH a directory containing the generated orders.tbl, lineitem.tbl, etc.",
        };
        PrintStream s = status == 0 ? System.out : System.err;
        for (String t : text) {
            s.println(t);
        }
        System.exit(status);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            displayHelp(1);
            return;
        }

        String connectString;
        String importPath;
        String scale_factor;
        switch (args[0]) {
            case "help":
                displayHelp(0);
                break;
            case "populate":
                if (args.length < 3) {
                    displayHelp(1);
                    return;
                }
                connectString = args[1];
                importPath = args[2];
                populate(connectString, importPath);
                break;
            case "evaluate":
                if (args.length < 3) {
                    displayHelp(1);
                    return;
                }
                connectString = args[1];
                scale_factor = args[2];
                evaluate(connectString, scale_factor);
                break;
            default:
                displayHelp(1);
                break;
        }
    }

    private static void populate(String connectString, String importPath) throws Exception {
        ConnectInfo connectInfo = ConnectInfo.parse(connectString);
        if (connectInfo == null) {
            displayHelp(1);
            return;
        }
        ensureDirectoryExists(importPath);
        connectInfo.getDatabaseSystem().populate(connectInfo, importPath);
    }

    private static void evaluate(String connectString, String scale_factor) throws RunnerException {
        ConnectInfo connectInfo = ConnectInfo.parse(connectString);
        if (connectInfo == null) {
            displayHelp(1);
            return;
        }

        Options opt = new OptionsBuilder()
                .include(TPCHBenchmark.class.getSimpleName())
                .timeUnit(TimeUnit.MILLISECONDS)
                .mode(Mode.AverageTime)
                .threads(1)
                .forks(1)
                .warmupIterations(1)
                .measurementIterations(3)
                .param("connectString", connectString)
                .param("scaleFactor", scale_factor)
                .build();

        //Hack to find org.openjdk.jmh.runner.ForkedMain class, because TPCHMain is called from maven exec plugin
        //https://stackoverflow.com/questions/35574688/how-to-run-a-jmh-benchmark-in-maven-using-execjava-instead-of-execexec
//        URLClassLoader classLoader = (URLClassLoader) nl.cwi.monetdb.benchmarks.TPCHBenchmark.class.getClassLoader();
//        StringBuilder classpath = new StringBuilder();
//        for (URL url : classLoader.getURLs())
//            classpath.append(url.getPath()).append(File.pathSeparator);
//        System.setProperty("java.class.path", classpath.toString());

        new Runner(opt).run();
    }

    private static void ensureDirectoryExists(String newDatabasePath) {
        File databaseDirectory = new File(newDatabasePath);
        if (!databaseDirectory.isAbsolute()) {
            TPCHMain.displayError(newDatabasePath + " is not a absolute path");
        }
        if (!databaseDirectory.exists()) {
            TPCHMain.displayError("Directory " + newDatabasePath + " does not exist");
        }
        if (!databaseDirectory.isDirectory()) {
            TPCHMain.displayError(newDatabasePath + " is not a directory");
        }
    }

}
