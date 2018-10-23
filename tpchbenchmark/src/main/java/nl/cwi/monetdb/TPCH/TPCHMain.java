package nl.cwi.monetdb.TPCH;

import nl.cwi.monetdb.Benchmarks.H2Benchmark;
import nl.cwi.monetdb.Benchmarks.MonetDBLiteBenchmark;
import nl.cwi.monetdb.Populate.H2Populate;
import nl.cwi.monetdb.Populate.MonetDBLitePopulate;
import nl.cwi.monetdb.Populate.TPCHPopulate;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

public class TPCHMain {

	public static void displayError(String message) {
		System.err.println(message);
		System.exit(1);
	}

	private static void displayHelp() {
		System.out.println("Usage of JVM TPC-H benchmark tool");
		System.out.println("Parameters: { populate | evaluate | help } other_parameters");
		System.out.println("For populate: { MonetDBLite-Java | H2 } database_path import_path");
		System.out.println("For evaluate: { MonetDBLite-Java | H2 } database_path");
		System.exit(0);
	}

	private static void populate(String[] args) throws Exception {
		TPCHPopulate popMe = null;

		if(args.length < 4) {
			displayError("Usage: populate { MonetDBLite-Java | H2 } database_path import_path");
		} else if(args[1].equals("MonetDBLite-Java")) {
			popMe = new MonetDBLitePopulate();
		} else if(args[1].equals("H2")) {
			popMe = new H2Populate();
		} else {
			displayError("Database " + args[1] + " not supported");
		}
		if(popMe != null){
			popMe.populate(args[2], args[3]);
		}
	}

	private static void evaluate(String[] args) throws RunnerException {
		Class testMe = null;

		if(args.length < 3) {
			displayError("Usage: evaluate { MonetDBLite-Java | H2 } database_path");
		} else if(args[1].equals("MonetDBLite-Java")) {
			testMe = MonetDBLiteBenchmark.class;
		} else if(args[1].equals("H2")) {
			testMe = H2Benchmark.class;
		} else {
			displayError("Database " + args[1] + " not supported");
		}
		if(testMe != null){
			ChainedOptionsBuilder opt = new OptionsBuilder()
				.include(testMe.getSimpleName())
				.timeUnit(TimeUnit.MILLISECONDS)
				.mode(Mode.SingleShotTime)
				.threads(1)
				.forks(1)
				.warmupIterations(0)
				.measurementIterations(1);

			for(int i = 1 ; i <= 3 ; i++) {
				System.out.println("Benchmark n" + i);
				new Runner(opt.build()).run();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if(args.length < 1) {
			displayError("The activity parameter must be provided: { populate | evaluate | help }");
		} else if(args[0].equals("help")) {
			displayHelp();
		} else if(args[0].equals("populate")) {
			populate(args);
		} else if(args[0].equals("evaluate")) {
			evaluate(args);
		} else {
			displayError("The activity parameter must be { populate | evaluate | help }");
		}
	}
}
