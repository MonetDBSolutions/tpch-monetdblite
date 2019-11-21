package nl.cwi.monetdb.TPCH;

import static org.junit.Assert.*;

public class ConnectInfoTest {

	@org.junit.Test
	public void parse() {
		assertParses("jdbc:postgresql:test", null, null, "jdbc:postgresql:test");
		assertParses("jvr@jdbc:postgresql:test", "jvr", null, "jdbc:postgresql:test");
		assertParses("jvr/pass@jdbc:postgresql:test", "jvr", "pass", "jdbc:postgresql:test");
	}

	private void assertParses(String connectString, String expectedUser, String expectedPassword, String expectedJdbcUrl) {
		ConnectInfo ci = ConnectInfo.parse(connectString);
		assertNotNull(ci);
		assertEquals(expectedUser, ci.getUser());
		assertEquals(expectedPassword, ci.getPassword());
		assertEquals(expectedJdbcUrl, ci.getJdbcUrl());
	}
}