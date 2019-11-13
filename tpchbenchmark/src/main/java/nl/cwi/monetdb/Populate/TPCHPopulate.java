package nl.cwi.monetdb.Populate;

import nl.cwi.monetdb.TPCH.TPCHMain;

import java.io.File;

public abstract class TPCHPopulate {

	abstract void populateInside(String databasePath, String dataPath) throws Exception;

	public void populate(String databasePath, String dataPath) throws Exception {
		File databaseDirectory = new File(databasePath);
		File dataDirectory = new File(dataPath);
		this.populateInside(databasePath, dataPath);
	}
}
