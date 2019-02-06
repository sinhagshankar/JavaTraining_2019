package com.stackroute.datamunger.query;

import java.util.*;

//This class will be used to store the column data types as columnIndex/DataType
public class RowDataTypeDefinitions extends HashMap<Integer, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<Integer,String> rowDataTypeDefinitionMap= new HashMap<Integer,String>();
	public Map<Integer, String> getRowDataTypeDefinitionMap() {
		return rowDataTypeDefinitionMap;
	}
	public void setRowDataTypeDefinitionMap(Map<Integer, String> rowDataTypeDefinitionMap) {
		this.rowDataTypeDefinitionMap = rowDataTypeDefinitionMap;
	}

}
