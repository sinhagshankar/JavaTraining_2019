package com.stackroute.datamunger.query;

import java.util.LinkedHashMap;

//This class will be acting as the DataSet containing multiple rows

public class DataSet extends LinkedHashMap<Long, Row> {

	private static final long serialVersionUID = 1L;
	LinkedHashMap<Long, Row> datasetLMap = new LinkedHashMap();
	public LinkedHashMap<Long, Row> getDatasetLMap() {
		return datasetLMap;
	}
	public void setDatasetLMap(LinkedHashMap<Long, Row> datasetLMap) {
		this.datasetLMap = datasetLMap;
	}
	/*
	 * The sort() method will sort the dataSet based on the key column with the help
	 * of Comparator
	 */
	public DataSet sort(RowDataTypeDefinitions dataTypes, String columnName) {

		return null;
	}

}
