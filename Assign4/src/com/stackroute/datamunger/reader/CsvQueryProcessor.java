package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	String fileName;
	BufferedReader reader;
	DataTypeDefinitions dataType;
	Header header;
	/*
	 * Parameterized constructor to initialize filename. As you are trying to
	 * perform file reading, hence you need to be ready to handle the IO Exceptions.
	 */

	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		this.header = new Header();
		this.reader = new BufferedReader(new FileReader(fileName));
		this.dataType = new DataTypeDefinitions();
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
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
		
		return header;
	}

	/**
	 * This method will be used in the upcoming assignments
	 */
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. In
	 * the previous assignment, we have tried to convert a specific field value to
	 * Integer or Double. However, in this assignment, we are going to use Regular
	 * Expression to find the appropriate data type of a field. Integers: should
	 * contain only digits without decimal point Double: should contain digits as
	 * well as decimal point Date: Dates can be written in many formats in the CSV
	 * file. However, in this assignment,we will test for the following date
	 * formats('dd/mm/yyyy',
	 * 'mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm
	 * -dd')
	 */

	@Override
	public DataTypeDefinitions getColumnType() throws IOException {

		// checking for Integer

		// checking for floating point numbers

		// checking for date format dd/mm/yyyy

		// checking for date format mm/dd/yyyy

		// checking for date format dd-mon-yy

		// checking for date format dd-mon-yyyy

		// checking for date format dd-month-yy

		// checking for date format dd-month-yyyy

		// checking for date format yyyy-mm-dd
		
		/*
		 * final String headerLine = reader.readLine(); String dataString = ""; if
		 * (headerLine != null) { dataString = reader.readLine(); } dataString += " ";
		 */
		
		String dataString = readAlineFromFile(2);
		dataString += " ";
		String[] dataArray = dataString.split(",");
		String[] result = new String[dataArray.length];

		for (int j = 0; j < dataArray.length; j++)
			if (dataArray[j].matches("[+-]?[0-9][0-9]*")) {
				int i = Integer.parseInt(dataArray[j]);
				result[j] = ((Object) i).getClass().getName();
			}else if (dataArray[j].matches("[+-]?[0-9]+(\\\\.[0-9]+)?([Ee][+-]?[0-9]+)?")) {
				result[j] = dataArray[j].getClass().getName();
			}else if (dataArray[j].matches("\\d{4}-\\d{2}-\\d{2}")) {
				SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-mm-dd");
				Date date = null;
				try {
					date = formatter1.parse(dataArray[j]);
				} catch (Exception e) {
					e.printStackTrace();
				}
				result[j] = ((Object) date).getClass().getName();
			}else if (dataArray[j].matches("\\s")) {
				result[j] = dataType.getClass().getSuperclass().getName();
			}
			else {
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
}
