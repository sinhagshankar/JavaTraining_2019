package com.stackroute.datamunger.query;

import java.util.*;

//Contains the row object as ColumnName/Value. Hence, HashMap is being used
public class Row extends HashMap<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String,String> rowMap= new HashMap<String, String>();
	public Map<String, String> getRowMap() {
		return rowMap;
	}
	public void setRowMap(Map<String, String> rowMap) {
		this.rowMap = rowMap;
	}
	
}
