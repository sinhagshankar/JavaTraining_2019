package com.stackroute.datamunger.reader;

import java.io.*;
import java.util.*;
import java.util.HashMap;

import com.stackroute.datamunger.query.*;
import com.stackroute.datamunger.query.parser.*;

/* This is the CsvGroupByAggregateQueryProcessor class used for evaluating queries with 
 * aggregate functions and group by clause*/
@SuppressWarnings("rawtypes")
public class CsvGroupByAggregateQueryProcessor implements QueryProcessingEngine {
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
		 * generated after processing a group by with aggregate clause is completely
		 * different from a dataSet structure(which contains multiple rows of data). In
		 * case of queries containing group by clause and aggregate functions, the
		 * resultSet will contain multiple dataSets, each of which will be assigned to
		 * the group by column value i.e. for all unique values of the group by column,
		 * aggregates will have to be calculated. Hence, we will use
		 * GroupedDataSet<String,Object> to store the same and not DataSet<Long,Row>.
		 * Please note we will process queries containing one group by column only for
		 * this example.
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
				String[] fieldVal=new String[fields.size()];
				for(int m=0;m<fields.size();m++) {
					if (fields.get(m).contains("sum") || fields.get(m).contains("max")
							|| fields.get(m).contains("min") || fields.get(m).contains("count") || fields.get(m).contains("avg")) {

						String[] split = (fields.get(m).replace("(", " ")).trim().split(" ");
						System.out.println(split[1].replace(")", "").trim());
						fieldVal[m]=split[1].replace(")", "").trim();
					}
					else {
						fieldVal[m]=fields.get(m);
					}
				}
				fields.clear();
				for(int m=0;m<fieldVal.length;m++) {
					fields.add(fieldVal[m]);
				}

				boolean addAllRows=false;
				if(fields.get(0).equals("*")) {


					for(int k=0;k<headers.length;k++ ) {
						r.put(headers[k],row[k] );
					}
				}
				else {
					//setting rows for fields
					for(int k=0;k<fields.size();k++) {
						if(fields.get(k).equals("*")) {
							addAllRows=true;
						}
						else {
							colIndexes.add(headerClass.get(fields.get(k)));
						}
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
					if(addAllRows) {
						for(int k=0;k<headers.length;k++ ) {
							r.put(headers[k],row[k] );
						}
					}
				}


				//r.setRow(rowData);
				//System.out.println("adding row data:"+r);
				dataSet.put((long)(rowCount), r)	;
				//System.out.println("DataSetMap:"+dataSetMap);
				rowCount++;
			}
		}
		GroupedDataSet groupedDataSet=getgroupedData(dataSet,queryParameter.getAggregateFunction(),headerClass,rowDataType,queryParameter.getGroupByFields());

		return groupedDataSet;
	}

	public static GroupedDataSet getgroupedData(DataSet dataset, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType,List<String> groupByFields) {
		GroupedDataSet groupedDataSet=new GroupedDataSet();

		for(int k=0;k<groupByFields.size();k++) {
			HashMap<String, List<Row>> groupedDataMap=new LinkedHashMap<String, List<Row>>();
			String groupByField=groupByFields.get(k);
			int groupByFieldIndex=header.get(groupByField);
			int distictRows=getDistinctValuesOfColumnFromDataSet(dataset,groupByField).size();
			List<List<Row>> rowLists=new ArrayList<List<Row>>();
			List<String> distinctValues=getDistinctValuesOfColumnFromDataSet(dataset,groupByField);
			for(int i=0;i<distictRows;i++) {
				rowLists.add(i, new ArrayList<Row>());
			}
			for(int i=0;i<dataset.size();i++) {
				int ListIndex=distinctValues.indexOf(dataset.get((long) (i+1)).get(groupByField));
				rowLists.get(ListIndex).add(dataset.get((long) (i+1)));
			}
			for(int i=0;i<rowLists.size();i++) {
				groupedDataMap.put(distinctValues.get(i), rowLists.get(i));
			}
			setValuesToGroupedDataSet(groupedDataSet,groupedDataMap,aggregateFunction,header,rowDataType);	

		}
		return groupedDataSet;
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

	public static GroupedDataSet setValuesToGroupedDataSet(GroupedDataSet groupeddataset,HashMap<String, List<Row>> groupedDataMap, List<AggregateFunction> aggregateFunction,
			Header header, RowDataTypeDefinitions rowDataType) {
		HashMap<String ,Object> map= new HashMap<String ,Object>();
		for (int j = 0; j < aggregateFunction.size(); j++) {
			String field = aggregateFunction.get(j).getField();
			String function = aggregateFunction.get(j).getFunction();
			List<String> distinctGroupedValues=new ArrayList<>();
			distinctGroupedValues.addAll(groupedDataMap.keySet());
			Set<String> headers=header.keySet();
			List<String> headersValues=new ArrayList<>();
			headersValues.addAll(headers);

			Integer num=0;
			Double d=0.00;
			if(field.equals("*")) {
				for(int k=0;k<groupedDataMap.size();k++) {
					for(int l=0;l<header.size();l++) {

						if ((rowDataType.get(header.get(headersValues.get(l)))).equals(num.getClass().getName())) {
							List<Integer> values = new ArrayList<>();

							for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {

								values.add(Integer.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(headersValues.get(l))));
							}
							IntSummaryStatistics summaryStatistics = values.stream().mapToInt((x) -> x).summaryStatistics();
							if (function.equals("min")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
							} else if (function.equals("max")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
							}
							else if (function.equals("sum")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
							}
							else if (function.equals("avg")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
							}
							else if (function.equals("count")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
							}
						}
						else	if ((rowDataType.get(header.get(headersValues.get(l)))).equals(d.getClass().getName())){
							List<Double> values = new ArrayList<>();
							for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {
								values.add(Double.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(headersValues.get(l))));
							}
							DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m).summaryStatistics();
							if (function.equals("min")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
							} else if (function.equals("max")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
							}
							else if (function.equals("sum")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
							}
							else if (function.equals("avg")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
							}
							else if (function.equals("count")) {
								groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
							}
						}
						else {
							if (function.equals("count")) {
								groupeddataset.put(distinctGroupedValues.get(k),groupedDataMap.get(distinctGroupedValues.get(k)).size());
							}
						}
					}	
				}

			}
			else {
				for(int k=0;k<groupedDataMap.size();k++) {
					if ((rowDataType.get(header.get(field))).equals(num.getClass().getName())) {
						List<Integer> values = new ArrayList<>();
						for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {

							values.add(Integer.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(field)));
						}
						IntSummaryStatistics summaryStatistics = values.stream().mapToInt((x) -> x).summaryStatistics();
						if (function.equals("min")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
						} else if (function.equals("max")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
						}
						else if (function.equals("sum")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
						}
						else if (function.equals("avg")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
						}
						else if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
						}
					}
					else if((rowDataType.get(header.get(field))).equals(d.getClass().getName())) {
						List<Double> values = new ArrayList<>();
						for (int i = 0; i <groupedDataMap.get(distinctGroupedValues.get(k)).size(); i++) {

							values.add(Double.valueOf(groupedDataMap.get(distinctGroupedValues.get(k)).get(i).get(field)));
						}
						DoubleSummaryStatistics summaryStatistics = values.stream().mapToDouble(m -> m).summaryStatistics();
						if (function.equals("min")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMin());
						} else if (function.equals("max")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getMax());
						}
						else if (function.equals("sum")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getSum());
						}
						else if (function.equals("avg")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getAverage());
						}
						else if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),summaryStatistics.getCount());
						}
					}
					else {
						if (function.equals("count")) {
							groupeddataset.put(distinctGroupedValues.get(k),groupedDataMap.get(distinctGroupedValues.get(k)).size());
						}
					}
				}
			}
		}
		return groupeddataset;
	}

}


