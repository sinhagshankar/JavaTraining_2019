package com.stackroute.datamunger.query;

import java.util.HashMap;

import com.google.gson.*;
import com.stackroute.datamunger.query.parser.QueryParameter;
import com.stackroute.datamunger.query.parser.QueryParser;
import com.stackroute.datamunger.reader.CsvAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByAggregateQueryProcessor;
import com.stackroute.datamunger.reader.CsvGroupByQueryProcessor;
import com.stackroute.datamunger.reader.CsvQueryProcessor;

@SuppressWarnings("rawtypes")
public class Query {

	/*
	 * This method will: 1.parse the query and populate the QueryParameter object
	 * 2.Based on the type of query, it will select the appropriate Query processor.
	 * In this example, we are going to work with only one Query Processor which is
	 * CsvQueryProcessor, which can work with select queries containing zero, one or
	 * multiple conditions
	 */
	public HashMap executeQuery(String queryString) {

		/* instantiate QueryParser class */

		/*
		 * call parseQuery() method of the class by passing the queryString which will
		 * return object of QueryParameter
		 */

		/*
		 * Check for Type of Query based on the QueryParameter object. In this
		 * assignment, we will process queries containing zero, one or multiple where
		 * conditions i.e. conditions, aggregate functions, order by, group by clause
		 */

		/*
		 * call the getResultSet() method of CsvQueryProcessor class by passing the
		 * QueryParameter Object to it. This method is supposed to return resultSet
		 * which is a HashMap
		 */

		QueryParser p=new QueryParser();

		QueryParameter parameter=p.parseQuery(queryString);

		HashMap returnMap=null;
		if((parameter.getAggregateFunction().size()!=0) && (parameter.getGroupByFields().size()!=0)) {
			CsvGroupByAggregateQueryProcessor csvGroupByAggregateQueryProcessor=new CsvGroupByAggregateQueryProcessor();
			returnMap=csvGroupByAggregateQueryProcessor.getResultSet(parameter);
		}
		else if(parameter.getAggregateFunction().size()!=0) {
			CsvAggregateQueryProcessor csvAggregateQueryProcessor=new CsvAggregateQueryProcessor();
			returnMap=csvAggregateQueryProcessor.getResultSet(parameter);
		}
		else if(parameter.getGroupByFields().size()!=0) {
			CsvGroupByQueryProcessor csvGroupByQueryProcessor=new CsvGroupByQueryProcessor();
			returnMap=csvGroupByQueryProcessor.getResultSet(parameter);
		}
		else {CsvQueryProcessor CsvProcessor=new CsvQueryProcessor();
		returnMap=CsvProcessor.getResultSet(parameter);
		}
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String result = gson.toJson(returnMap);
		System.out.println("data set:"+result);
		return returnMap;
	}

}

