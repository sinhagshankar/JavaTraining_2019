package com.stackroute.datamunger.reader;

import java.io.*;
import java.util.*;
import java.util.HashMap;

import com.stackroute.datamunger.query.*;
import com.stackroute.datamunger.query.parser.*;

/* This is the CsvGroupByQueryProcessor class used for evaluating queries without 
 * aggregate functions but with group by clause*/
@SuppressWarnings("rawtypes")
public class CsvGroupByQueryProcessor implements QueryProcessingEngine {
	/*
	 * This method will take QueryParameter object as a parameter which contains the
	 * parsed query and will process and populate the ResultSet
	 */
	public HashMap getResultSet(QueryParameter queryParameter) {

		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */

		/*
		 * read the first line which contains the header. Please note that the headers
		 * can contain spaces in between them. For eg: city, winner
		 */

		/*
		 * read the next line which contains the first row of data. We are reading this
		 * line so that we can determine the data types of all the fields. Please note
		 * that ipl.csv file contains null value in the last column. If you do not
		 * consider this while splitting, this might cause exceptions later
		 */

		/*
		 * populate the header Map object from the header array. header map is having
		 * data type <String,Integer> to contain the header and it's index.
		 */

		/*
		 * We have read the first line of text already and kept it in an array. Now, we
		 * can populate the dataTypeDefinition Map object. dataTypeDefinition map is
		 * having data type <Integer,String> to contain the index of the field and it's
		 * data type. To find the dataType by the field value, we will use getDataType()
		 * method of DataTypeDefinitions class
		 */

		/*
		 * once we have the header and dataTypeDefinitions maps populated, we can start
		 * reading from the first line. We will read one line at a time, then check
		 * whether the field values satisfy the conditions mentioned in the query,if
		 * yes, then we will add it to the resultSet. Otherwise, we will continue to
		 * read the next line. We will continue this till we have read till the last
		 * line of the CSV file.
		 */

		/* reset the buffered reader so that it can start reading from the first line */

		/*
		 * skip the first line as it is already read earlier which contained the header
		 */

		/* read one line at a time from the CSV file till we have any lines left */

		/*
		 * once we have read one line, we will split it into a String Array. This array
		 * will continue all the fields of the row. Please note that fields might
		 * contain spaces in between. Also, few fields might be empty.
		 */

		/*
		 * if there are where condition(s) in the query, test the row fields against
		 * those conditions to check whether the selected row satifies the conditions
		 */

		/*
		 * from QueryParameter object, read one condition at a time and evaluate the
		 * same. For evaluating the conditions, we will use evaluateExpressions() method
		 * of Filter class. Please note that evaluation of expression will be done
		 * differently based on the data type of the field. In case the query is having
		 * multiple conditions, you need to evaluate the overall expression i.e. if we
		 * have OR operator between two conditions, then the row will be selected if any
		 * of the condition is satisfied. However, in case of AND operator, the row will
		 * be selected only if both of them are satisfied.
		 */

		/*
		 * check for multiple conditions in where clause for eg: where salary>20000 and
		 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
		 */

		/*
		 * if the overall condition expression evaluates to true, then we need to check
		 * for the existence for group by clause in the Query Parameter. The dataSet
		 * generated after processing a group by clause is completely different from a
		 * dataSet structure(which contains multiple rows of data). In case of queries
		 * containing group by clause, the resultSet will contain multiple dataSets,
		 * each of which will be assigned to the group by column value i.e. for all
		 * unique values of the group by column, there can be multiple rows associated
		 * with it. Hence, we will use GroupedDataSet<String,Object> to store the same
		 * and not DataSet<Long,Row>. Please note we will process queries containing one
		 * group by column only for this example.
		 */

		// return groupedDataSet object

		DataSet dataSet=new DataSet();
		/*
		 * initialize BufferedReader to read from the file which is mentioned in
		 * QueryParameter. Consider Handling Exception related to file reading.
		 */


		String fileName = queryParameter.getFileName();
		Header headerClass = new Header();
		RowDataTypeDefinitions rowDataType = new RowDataTypeDefinitions();
		FileReader file;
		String header1 = null;
		String row1 = null;
		List<String> rows = new ArrayList<>();
		/*
		 * read the first line which contains the header. Please note that the headers
		 * can contain spaces in between them. For eg: city, winner
		 */

		try {
			file = new FileReader(fileName);
			final BufferedReader br = new BufferedReader(file);
			while ((row1 = br.readLine()) != null) {

				rows.add(row1);
			}

			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//Reading header
		header1 = rows.get(0);
		String[] headers = header1.split(",");
		//HashMap<String, Integer> header = new HashMap<String, Integer>();
		for (int i = 0; i < headers.length; i++) {
			headerClass.put(headers[i].trim(), i + 1);
		}
		//headerClass.setHeader(header);

		System.out.println("---headers----");
		System.out.println(Arrays.toString(headers));

		//Reading data type and assigning to RowDataTypeDefinition
		row1 = rows.get(1);
		HashMap<Integer, String> rowDataTypeMap = new HashMap<Integer, String>();
		if (row1 != null) {
			if (row1.lastIndexOf(",") == (row1.length() - 1)) {
				row1 = row1 + " ";
			}
			final String[] data = row1.split(",");
			System.out.println("row:" + row1);

			for (int i = 0; i < data.length; i++) {

				rowDataType.put(i + 1, (DataTypeDefinitions.getDataType(data[i])).toString());
			}
		}
		//rowDataType.setRowDataTypeDefinitions(rowDataTypeMap);
		// Evaluating the row

		LinkedHashMap<Long, Row> dataSetMap=new LinkedHashMap<Long, Row>(); 
		int rowCount=1;
		List<Integer> rowIndexes=new ArrayList<>();
		for (int i = 1; i < rows.size(); i++) {
			Row r= new Row();
			String line = rows.get(i);
			//System.out.println("aa");
			if (line.lastIndexOf(",") == (line.length() - 1)) {
				line = line + " ";
			}
			String[] row = line.split(",");
			for(int l=0;l<row.length;l++) {
				row[l]=row[l].trim();
			}
			//Filtering row
			Boolean flag =null;
			if(queryParameter.getRestrictions()!=null) {
				if(i==243) {
					System.out.println("aa");
				}
				flag = Filter.evaluateExpression(queryParameter.getRestrictions(), queryParameter.getLogicalOperators(), row,
						headerClass,rowDataType);
			}
			else {
				flag=true;
			}
			//Initializing Row class 
			if(flag) {
				List<Integer> colIndexes=new ArrayList<>();
				List<String> fields=queryParameter.getFields();
				if(fields.get(0).equals("*")) {


					for(int k=0;k<headers.length;k++ ) {
						r.put(headers[k],row[k] );
					}
				}
				else {
					//setting rows for fields
					for(int k=0;k<fields.size();k++) {

						colIndexes.add(headerClass.get(fields.get(k)));
					}
					try {
						Collections.sort(colIndexes);
					}
					catch (Exception e) {
						// TODO: handle exception
					}
					for(int k=0;k<colIndexes.size();k++ ) {
						r.put(headers[colIndexes.get(k)-1],row[colIndexes.get(k)-1] );
					}

				}
				rowIndexes.add(i);
				//r.setRow(rowData);
				//System.out.println("adding row data:"+r);
				dataSet.put((long)(rowCount), r)	;
				//System.out.println("DataSetMap:"+dataSetMap);
				rowCount++;
			}
		}
		HashMap<String, HashMap<Long, Row>> groupedHashMap=	getgroupedData(dataSet,queryParameter.getAggregateFunction(),headerClass,rowDataType,queryParameter.getGroupByFields(),rowIndexes);
		return groupedHashMap;



	}

	public static HashMap<String, HashMap<Long, Row>>  getgroupedData(DataSet dataset, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType,List<String> groupByFields,List<Integer> filteredRowIndexes) {

		HashMap<String, HashMap<Long, Row>> groupedDataMap=new LinkedHashMap<String, HashMap<Long, Row>>();
		for(int k=0;k<groupByFields.size();k++) {

			String groupByField=groupByFields.get(k);
			int groupByFieldIndex=header.get(groupByField);
			int distictRows=getDistinctValuesOfColumnFromDataSet(dataset,groupByField).size();
			//List<List<Row>> rowLists=new ArrayList<List<Row>>();
			List<LinkedHashMap<Long, Row>> dataMaps=new ArrayList<LinkedHashMap<Long,Row>>();

			List<String> distinctValues=getDistinctValuesOfColumnFromDataSet(dataset,groupByField);
			for(int i=0;i<distictRows;i++) {
				dataMaps.add(new LinkedHashMap<Long, Row>());
			}
			for(int i=0;i<dataset.size();i++) {
				int ListIndex=distinctValues.indexOf(dataset.get((long) (i+1)).get(groupByField));
				dataMaps.get(ListIndex).put((long)(filteredRowIndexes.get(i)),dataset.get((long) (i+1)));
			}
			for(int i=0;i<dataMaps.size();i++) {
				groupedDataMap.put(distinctValues.get(i), dataMaps.get(i));
			}


		}
		return 	groupedDataMap;
	}

	public static List<String> getDistinctValuesOfColumnFromDataSet(DataSet dataset,String field) {
		HashSet<String> hashset= new HashSet<String>();
		List<String> distinctValues=new ArrayList<>();
		for(int i=0;i<dataset.size();i++) {
			hashset.add(dataset.get((long)(i+1)).get(field));

		}
		distinctValues.addAll(hashset);
		return distinctValues;

	}

}
