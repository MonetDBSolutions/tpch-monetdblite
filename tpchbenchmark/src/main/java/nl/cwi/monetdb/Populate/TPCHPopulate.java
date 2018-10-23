package nl.cwi.monetdb.Populate;

import nl.cwi.monetdb.TPCH.TPCHMain;

import java.io.File;

public abstract class TPCHPopulate {

	abstract void populateInside(String databasePath, String dataPath) throws Exception;

	abstract String setDatabasePathInternal(String databasePath) throws Exception;

	public void populate(String databasePath, String dataPath) throws Exception {
		File databaseDirectory = new File(databasePath);
		if(!databaseDirectory.isAbsolute()) {
			TPCHMain.displayError(databasePath + " is not a absolute path");
		}
		if(!databaseDirectory.exists()) {
			TPCHMain.displayError("Directory " + databasePath + " does not exist");
		}
		if(!databaseDirectory.isDirectory()) {
			TPCHMain.displayError(databasePath + " is not a directory");
		}
		databasePath = this.setDatabasePathInternal(databasePath);

		File dataDirectory = new File(dataPath);
		if(!dataDirectory.isAbsolute()) {
			TPCHMain.displayError(dataPath + " is not a absolute path");
		}
		if(!dataDirectory.exists()) {
			TPCHMain.displayError("Directory " + dataPath + " does not exist");
		}
		if(!dataDirectory.isDirectory()) {
			TPCHMain.displayError(dataPath + " is not a directory");
		}

		this.populateInside(databasePath, dataPath);
	}
}
