# skip 'mvn package' if nothing changed.

TOOL_VERSION=1.0
JAVA_CODE=tpchbenchmark
JAR=$(JAVA_CODE)/target/tpchbenchmark-$(TOOL_VERSION).jar

jar: $(JAR)

$(JAR): $(JAVA_CODE)/pom.xml $(shell find $(JAVA_CODE)/src -type f)
	cd "$(JAVA_CODE)" && mvn package
