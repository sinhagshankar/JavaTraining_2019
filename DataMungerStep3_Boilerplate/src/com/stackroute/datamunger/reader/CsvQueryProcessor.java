package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	String fileName;
	BufferedReader reader;
	DataTypeDefinitions dataType;
	Header header;
	// Parameterized constructor to initialize filename
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		this.header = new Header();
		this.reader = new BufferedReader(new FileReader(fileName));
		this.dataType = new DataTypeDefinitions();
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */

	@Override
	public Header getHeader() throws IOException {
		/*
		 * reader.mark(1); if (header.getHeaders() == null) {
		 * 
		 * String headString = reader.readLine(); String[] headArray =
		 * headString.trim().split(","); header.setHeaders(headArray); } // populate the
		 * header object with the String array containing the header names
		 * reader.reset();
		 */
		if (header.getHeaders() == null) {

			String[] headArray = readAlineFromFile(1).trim().split(",");
			header.setHeaders(headArray);
		}
		// populate the header object with the String array containing the header names
		//reader.reset();
		return header;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */

	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. 
	 * Note: Return Type of the method will be DataTypeDefinitions
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {
		
		/*
		 * final String headerLine = reader.readLine(); String dataString = ""; if
		 * (dataString != null) { dataString = reader.readLine(); } dataString += " ";
		 * String[] dataArray = dataString.split(",");
		 */
		
		String dataString = readAlineFromFile(2);
		dataString += " ";
		String[] dataArray = dataString.split(",");
		String[] result = new String[dataArray.length];
		for (int j = 0; j < dataArray.length; j++)
			if (isStringInt(dataArray[j])) {
				int i = Integer.parseInt(dataArray[j]);
				result[j] = ((Object) i).getClass().getName();
			} else {
				result[j] = dataArray[j].getClass().getName();
			}
		dataType.setDataTypes(result);
		return dataType;
	}


	//public String[] readAlineFromFile(int rowCount) throws IOException { // We cann't return String[] because of getColumnType has value empty(null) for 3rd Umpire
	public String readAlineFromFile(int rowCount) throws IOException {
		String str="";
		//StringBuffer sb = new StringBuffer();
		int count = 1;
		reader.mark(1);
		while ((str = reader.readLine()) != null) {
			// read the second line
			if (count == rowCount) {
				//sb.append(str).append(" ,");
				break;
			}
			count++;
		}
		reader.reset();
		//str = sb.toString();
		//return str.trim().split(",");
		return str;
	}

	public boolean isStringInt(String s) {
		boolean isInt = false;
		try {
			Integer.parseInt(s);
			isInt = true;
		} catch (NumberFormatException nfe) {
			isInt = false;
		}
		return isInt;
	}

	public boolean isStringDouble(String s) {
		boolean isDouble = false;
		try {
			Double.parseDouble(s);
			isDouble = true;

		} catch (NumberFormatException ex) {
			isDouble = false;
		}
		return isDouble;
	}
}
