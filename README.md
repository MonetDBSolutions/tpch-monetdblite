# tpch-monetdblite
Run TPC-H benchmarks on MonetDBLite and other databases for the JVM.

## Dependencies:
- Cmake (used by tpch-dbgen)
- OpenJDK 1.8
- Apache Maven (must be set on `$PATH` environment variable)
- GNU Make (used by tpch-dbgen)
- GCC compiler
- GNU Core Utilities
- An Unix shell (I use bash)

## How to run

Don't forget to init the repository's submodule : `git clone --recurse-submodules ...` or after cloning, 
`git submodule update --init --recursive`.

Currently, Maxine VM's compiler has several bugs and OpenJDK's one should be used instead. The `compiler`
profile will set the directory for the right compiler to use. At the same time, I'm using a SNAPSHOT
release for MonetDBLite-Java, and Sonatype's snapshot repository is not enabled by default. The 
`allow-snapshots` repository enables it.

On the `settings.xml` file of Apache Maven add the two profiles:

    <profile>
      <id>compiler</id>
      <properties>
        <JAVACOMPILEHOME>@@REPLACE_ME_WITH_JAVAC_DIRECTORY@@</JAVACOMPILEHOME>
      </properties>
    </profile>
    <profile>
      <id>allow-snapshots</id>
      <activation><activeByDefault>true</activeByDefault></activation>
      <repositories>
        <repository>
          <id>snapshots-repo</id>
          <url>https://oss.sonatype.org/content/repositories/snapshots</url>
          <releases><enabled>true</enabled></releases>
          <snapshots><enabled>true</enabled></snapshots>
        </repository>
      </repositories>
    </profile>

Also set them to active under the active profiles tab:

    <activeProfile>compiler</activeProfile>
    <activeProfile>allow-snapshots</activeProfile>

The `$JAVA_HOME` environment variable, must be set with the path of the JDK with the JVM to benchmark.

On the cloned tpch-monetdblite directory the following scripts are available:

- `generate_dataset.sh` for importing:
```sh
./generate_dataset.sh --sf 1 --database { MonetDBLite-Java | H2 } --path <Absolute path of MonetDBLite-Java database farm>
```

- `run_tpch_queries.sh` for TPC-H benchmark run:
```sh
./run_tpch_queries.sh --sf 1 --database { MonetDBLite-Java | H2 } --path  <Absolute path of MonetDBLite-Java database farm>
```
