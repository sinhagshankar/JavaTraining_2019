package com.stackroute.datamunger;

import java.util.ArrayList;
import java.util.List;

/*There are total 5 DataMungertest files:
 * 
 * 1)DataMungerTestTask1.java file is for testing following 3 methods
 * a)getSplitStrings()  b) getFileName()  c) getBaseQuery()
 * 
 * Once you implement the above 3 methods,run DataMungerTestTask1.java
 * 
 * 2)DataMungerTestTask2.java file is for testing following 3 methods
 * a)getFields() b) getConditionsPartQuery() c) getConditions()
 * 
 * Once you implement the above 3 methods,run DataMungerTestTask2.java
 * 
 * 3)DataMungerTestTask3.java file is for testing following 2 methods
 * a)getLogicalOperators() b) getOrderByFields()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask3.java
 * 
 * 4)DataMungerTestTask4.java file is for testing following 2 methods
 * a)getGroupByFields()  b) getAggregateFunctions()
 * 
 * Once you implement the above 2 methods,run DataMungerTestTask4.java
 * 
 * Once you implement all the methods run DataMungerTest.java.This test case consist of all
 * the test cases together.
 */

public class DataMunger {

	public static boolean empty(final String s) {
		// Null-safe, short-circuit evaluation.
		return s == null || s.trim().isEmpty();
	}

	/*
	 * This method will split the query string based on space into an array of words
	 * and display it on console
	 */
	public String[] getSplitStrings(String queryString) {
		if (!empty(queryString)) {
			return queryString.toLowerCase().split(" ");
		} else {
			return null;
		}
	}

	/*
	 * Extract the name of the file from the query. File name can be found after a
	 * space after "from" clause. Note: ----- CSV file can contain a field that
	 * contains from as a part of the column name. For eg: from_date,from_hrs etc.
	 * 
	 * Please consider this while extracting the file name in this method.
	 */

	/*
	 * public String getFileName(String queryString) {
	 * 
	 * if(!empty(queryString)){ return
	 * queryString.substring(queryString.indexOf("from ")+("from".length()+1)).
	 * split(" ")[0]; }else { return null; } }
	 */

	public String getFileName(String queryString) {
		String strFrom = queryString.split("from")[1].trim();
		String strFileName = strFrom.split(" ")[0].trim();
		return strFileName;
	}

	/*
	 * This method is used to extract the baseQuery from the query string. BaseQuery
	 * contains from the beginning of the query till the where clause
	 * 
	 * Note: ------- 1. The query might not contain where clause but contain order
	 * by or group by clause 2. The query might not contain where, order by or group
	 * by clause 3. The query might not contain where, but can contain both group by
	 * and order by clause
	 */

	/*
	 * public String getBaseQuery(String queryString) { if(!empty(queryString)){
	 * String lwqString = queryString.toLowerCase(); int indexOf =
	 * lwqString.indexOf("where"); if (indexOf < 0) { indexOf =
	 * lwqString.indexOf("group by"); } if (indexOf < 0) { indexOf =
	 * lwqString.indexOf("order by"); } if (indexOf > -1) { return
	 * lwqString.substring(0, indexOf - 1); } else { return queryString; } } return
	 * ""; }
	 */

	public String getBaseQuery(String queryString) {
		String strBaseQuery = "";
		if (queryString.contains("where")) {
			strBaseQuery = queryString.toLowerCase().split("where")[0].trim();
		} else if (queryString.contains("group by") || queryString.contains("order by")) {
			strBaseQuery = queryString.toLowerCase().split("group by|order by")[0].trim();
		} else {
			strBaseQuery = queryString;
		}
		return strBaseQuery;
	}

	/*
	 * This method will extract the fields to be selected from the query string. The
	 * query string can have multiple fields separated by comma. The extracted
	 * fields will be stored in a String array which is to be printed in console as
	 * well as to be returned by the method
	 * 
	 * Note: 1. The field name or value in the condition can contain keywords as a
	 * substring. For eg: from_city,job_order_no,group_no etc. 2. The field name can
	 * contain '*'
	 * 
	 */

	/*
	 * public String[] getFields(String queryString) { String array [] = null; if
	 * (!empty(queryString)) { String lowerCaseQueryString =
	 * queryString.toLowerCase(); int startIndex =
	 * lowerCaseQueryString.indexOf("select"); int endIndex =
	 * lowerCaseQueryString.indexOf("from"); return
	 * lowerCaseQueryString.substring(startIndex + "select ".length(), endIndex -
	 * 1).split(","); } return array; }
	 */

	public String[] getFields(String queryString) {
		String strSelect = queryString.toLowerCase().split("select")[1].trim();
		String strFrom = strSelect.split("from")[0].trim();
		String[] strField = strFrom.split(",");
		return strField;
	}

	/*
	 * This method is used to extract the conditions part from the query string. The
	 * conditions part contains starting from where keyword till the next keyword,
	 * which is either group by or order by clause. In case of absence of both group
	 * by and order by clause, it will contain till the end of the query string.
	 * Note: 1. The field name or value in the condition can contain keywords as a
	 * substring. For eg: from_city,job_order_no,group_no etc. 2. The query might
	 * not contain where clause at all.
	 */

	/*
	 * public String getConditionsPartQuery(String queryString) {
	 * 
	 * if(!empty(queryString)){ return
	 * queryString.toLowerCase().substring(queryString.indexOf("where")+("where".
	 * length()+1)); }else { return null; } if (!empty(queryString)) { String
	 * lowerCaseQueryString = queryString.toLowerCase(); int startIndex =
	 * lowerCaseQueryString.indexOf("where"); if (startIndex > -1) { int endIndex =
	 * lowerCaseQueryString.indexOf("group"); if (endIndex < 0) { endIndex =
	 * lowerCaseQueryString.indexOf("order"); } if (endIndex < 0) { return
	 * lowerCaseQueryString.substring(startIndex + "where ".length()); } return
	 * lowerCaseQueryString.substring(startIndex + "where ".length(), endIndex - 1);
	 * } } return null; }
	 */

	public String getConditionsPartQuery(String queryString) {
		String strConditions = "";
		if(queryString.contains("where")) {
			strConditions = queryString.toLowerCase().trim().split("where|group by|order by")[1].trim();
		}else {
			strConditions = null;
		}
		return strConditions;
	}


	/*
	 * This method will extract condition(s) from the query string. The query can
	 * contain one or multiple conditions. In case of multiple conditions, the
	 * conditions will be separated by AND/OR keywords. for eg: Input: select
	 * city,winner,player_match from ipl.csv where season > 2014 and city
	 * ='Bangalore'
	 * 
	 * This method will return a string array ["season > 2014","city ='bangalore'"]
	 * and print the array
	 * 
	 * Note: ----- 1. The field name or value in the condition can contain keywords
	 * as a substring. For eg: from_city,job_order_no,group_no etc. 2. The query
	 * might not contain where clause at all.
	 */

	public String[] getConditions(String queryString) {

		String array[] = null;
		String conditionsPartQuery = getConditionsPartQuery(queryString);
		if (!empty(conditionsPartQuery)) {
			return conditionsPartQuery.toLowerCase().split(" and | or ");
		}
		return array;
	}

	/*
	 * This method will extract logical operators(AND/OR) from the query string. The
	 * extracted logical operators will be stored in a String array which will be
	 * returned by the method and the same will be printed Note: 1. AND/OR keyword
	 * will exist in the query only if where conditions exists and it contains
	 * multiple conditions. 2. AND/OR can exist as a substring in the conditions as
	 * well. For eg: name='Alexander',color='Red' etc. Please consider these as well
	 * when extracting the logical operators.
	 * 
	 */

	/*
	 * public String[] getLogicalOperators(String queryString) {
	 * 
	 * String arr[] = null; String result = ""; if
	 * (queryString.toLowerCase().indexOf(" and ") > -1) { result =
	 * result.concat("and="); } if (queryString.toLowerCase().indexOf(" or ") > -1)
	 * { result = result.concat("or"); } if (!result.isEmpty()) { return
	 * result.split("="); } return arr; }
	 */

	public String[] getLogicalOperators(String queryString) {
		String str = queryString.toLowerCase();
		String[] strAndOr = null;
		String[] strOperator = new String[100];
		if(str.contains("where")) {
			String strwhere = str.split("where")[1].trim();
			strAndOr = strwhere.split(" ");
			int j = 0;
			for(int i = 0;i < strAndOr.length;i++) {
				if(strAndOr[i].equals("and")||strAndOr[i].equals("or")) {
					strOperator[j] = strAndOr[i];
					j++;
				}
			}
			//return strOperator; is containing an array of length 100, we need to put it in another array to make it work
			String[] returnOperator = new String[j];
			for(int i = 0;i < j;i++) {
				returnOperator[i] = strOperator[i];			
			}
			return returnOperator;
		}
		else {
			return null;
		}	
	}

	/*
	 * This method extracts the order by fields from the query string. Note: 1. The
	 * query string can contain more than one order by fields. 2. The query string
	 * might not contain order by clause at all. 3. The field names,condition values
	 * might contain "order" as a substring. For eg:order_number,job_order Consider
	 * this while extracting the order by fields
	 */

	/*
	 * public String[] getOrderByFields(String queryString) {
	 * 
	 * if (!empty(queryString)) { queryString = queryString.toLowerCase(); int
	 * startIndex = queryString.indexOf("order by"); if (startIndex > -1) { return
	 * queryString.substring(startIndex + "order by ".length()).split(" "); } }
	 * return null; }
	 */

	public String[] getOrderByFields(String queryString) {
		String str = queryString.toLowerCase();
		String[] strOrderByFields = null;
		if(str.contains("where")&&(str.contains("order by"))) {
			String strWhere = str.split("where")[1].trim();
			String strOrderByString = strWhere.split("order by")[1].trim();
			strOrderByFields = strOrderByString.split(",");
		}else if(str.contains("order by")){
			String strNotWhere = str.split("order by")[1].trim();
			strOrderByFields = strNotWhere.split(",");
		}
		return strOrderByFields;
	}

	/*
	 * This method extracts the group by fields from the query string. Note: 1. The
	 * query string can contain more than one group by fields. 2. The query string
	 * might not contain group by clause at all. 3. The field names,condition values
	 * might contain "group" as a substring. For eg: newsgroup_name
	 * 
	 * Consider this while extracting the group by fields
	 */

	/*
	 * public String[] getGroupByFields(String queryString) { String array[] = null;
	 * if (!empty(queryString)) { queryString = queryString.toLowerCase(); int
	 * startIndex = queryString.indexOf("group by"); if (startIndex > -1) { int
	 * endIndex = queryString.indexOf("order"); if (endIndex < 0) { return
	 * queryString.substring(startIndex + "groub by ".length()).split(","); } return
	 * queryString.substring(startIndex + "where ".length(), endIndex -
	 * 1).split(","); } } return array; }
	 */

	public String[] getGroupByFields(String queryString) {
		String str = queryString.toLowerCase();
		//String[] strGroupByFields = new String[] {};
		String[] strGroupByFields = null;
		List<String> result=new ArrayList<String>();
		if(str.contains("where")&&(str.contains("group by"))&&str.contains("order by")) {
			String whereString = str.split("where")[1].trim();
			String groupByString = whereString.split("group by")[1].trim();
			String strbeforeOrderBy= groupByString.split("order by")[0].trim();
			if(strbeforeOrderBy.contains(",")) {
				strGroupByFields = strbeforeOrderBy.split(",");
			}else {
				strGroupByFields = new String[] {strbeforeOrderBy};
			}
		}else if(str.contains("where")&&(str.contains("group by"))) {
			String strWhere = str.split("where")[1].trim();
			String strOrderByString = strWhere.split("group by")[1].trim();
			strGroupByFields = strOrderByString.split(",");
		}else if(str.contains("group by")){
			String strNotWhere = str.split("group by")[1].trim();
			strGroupByFields = strNotWhere.split(",");
		}
		/* Not required ..instead initialize the property as null
		 * else { strGroupByFields = null; }
		 */
		return strGroupByFields;
	}

	/*
	 * This method extracts the aggregate functions from the query string. Note: 1.
	 * aggregate functions will start with "sum"/"count"/"min"/"max"/"avg" followed
	 * by "(" 2. The field names might contain"sum"/"count"/"min"/"max"/"avg" as a
	 * substring. For eg: account_number,consumed_qty,nominee_name
	 * 
	 * Consider this while extracting the aggregate functions
	 */

	public String[] getAggregateFunctions(String queryString) {

		String array[] = null;
		String[] fields = getFields(queryString);
		if (!empty(queryString)) {
			StringBuilder builder = new StringBuilder();
			for (String field : fields) {
				if (field.indexOf("(") > -1) {
					builder.append(field).append("=");
				}
			}
			return (builder.length() > 0 ? builder.toString().split("=") : null);
		}
		return array;
	}

}