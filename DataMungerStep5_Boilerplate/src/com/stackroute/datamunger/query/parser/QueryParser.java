package com.stackroute.datamunger.query.parser;

import java.util.*;

public class QueryParser {

	final private QueryParameter queryParameter = new QueryParameter();

	private static final String GROUP= "group";
	private static final String ORDER= "order";
	private static final String GRORBY= "by";
	private static final String WHERE= "where";
	private static final String REGW="[\\W]+";
	/*
	 * This method will parse the queryString and will return the object of
	 * QueryParameter class
	 */
	public QueryParameter parseQuery(final String queryString) {

		queryParameter.setBaseQuery(getBaseQuery(queryString));
		queryParameter.setFileName(getFileName(queryString));
		queryParameter.setOrderByFields(getOrderByFields(queryString));
		queryParameter.setGroupByFields(getGroupByFields(queryString));
		queryParameter.setFields(getFields(queryString));
		queryParameter.setLogicalOperators(getLogicalOperators(queryString));
		queryParameter.setAggregateFunctions(getAggregateFunctions(queryString));
		queryParameter.setRestrictions(getRestrictions(queryString));

		return queryParameter;		
	}


	/*
	 * Splitting the strings to parts
	 * @param queryString
	 * @return
	 */

	public String[] getSplitStrings(final String queryString) {
		final String queryLowerCase=queryString.toLowerCase(Locale.ENGLISH);
		return queryLowerCase.split(" ");
	}

	/*
	 * Extract the name of the file from the query. File name can be found after the
	 * "from" clause.
	 * @param queryString
	 * @return
	 */
	public String getFileName(String queryString) {
		String strFrom = queryString.split("from")[1].trim();
		String strFileName = strFrom.split(" ")[0].trim();
		return strFileName;
	}

	/*
	 * Extract the baseQuery from the query.This method is used to extract the
	 * baseQuery from the query string. BaseQuery contains from the beginning of the
	 * query till the where clause
	 * @param queryString
	 * @return
	 */

	public String getBaseQuery(final String queryString) {
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
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 * @param queryString
	 * @return
	 */

	public List<String> getOrderByFields(final String queryString) {
		String str = queryString.toLowerCase();
		String[] strOrderByFields = new String[] {};
		List<String> result=new ArrayList<String>();
		if(str.contains("where")&&(str.contains("order by"))) {
			String strWhere = str.split("where")[1].trim();
			String strOrderByString = strWhere.split("order by")[1].trim();
			strOrderByFields = strOrderByString.split(",");
		}else if(str.contains("order by")){
			String strNotWhere = str.split("order by")[1].trim();
			strOrderByFields = strNotWhere.split(",");
		}
		for (String string : strOrderByFields) {
			result.add(string);
		}
		return result;
	}


	/*
	 * Extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 * @param queryString
	 * @return
	 */

	public List<String> getGroupByFields(final String queryString) {
		String str = queryString.toLowerCase();
		//String[] strGroupByFields = null; // will result in NPE when iterate over it..hence need to  initialize it
		String[] strGroupByFields = new String[] {};
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
		} /*
		 * else { strGroupByFields = null; }
		 */
		for (String string : strGroupByFields) {
			result.add(string);
		}
		return result;
	}


	/*
	 * Extract the selected fields from the query string. Please note that we will
	 * need to extract the field(s) after "select" clause followed by a space from
	 * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
	 * query mentioned above, we need to extract "city" and "win_by_runs". Please
	 * note that we might have a field containing name "from_date" or "from_hrs".
	 * Hence, consider this while parsing.
	 * @param queryString
	 * @return
	 */

	public List<String> getFields(final String queryString) {
		List<String> result=new ArrayList<String>();
		String strSelect = queryString.toLowerCase().split("select")[1].trim();
		String strFrom = strSelect.split("from")[0].trim();
		String[] strField = strFrom.split(",");
		for (String string : strField) {
			result.add(string);
		}
		return result;
	}

	/*
	 * Extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 1. Name of field 2. condition 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 1. Name of
	 * field: season 2. condition: >= 3. value: 2008
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions.
	 * @param queryString
	 * @return
	 */

	public List<Restriction> getRestrictions(final String queryString) {
		final String [] query = queryString.split(" ");
		final StringBuffer output = new StringBuffer();
		List<Restriction> liresult=null;
		String result[]=null;
		if(queryString.contains(" where ")) {
			for(int i=0;i<query.length;i++) {
				if(query[i].contains(WHERE)){
					for(int j=i+1;j<query.length;j++) {
						if(query[j].equals(ORDER) || query[j].equals(GROUP) && query[j+1].equals(GRORBY)) {
							break;
						}
						output.append(query[j]);
						output.append(' ');
					}
					break;
				}
			}
			result= output.toString().trim().split(" and | or |;");
			if(result!=null) {
				liresult=new ArrayList<Restriction>();
				for(int i=0;i<result.length;i++) {
					if(result[i].contains(">=")) {
						final String[] res = result[i].split(REGW);
						//final Restriction restriction = new Restriction(res[0].trim(),res[1].trim(),">="); // Not the best case as we've setters already
						// is the best practice, but test cases are written like that
						  final Restriction restriction = new Restriction();
						restriction.setPropertyName(res[0].trim());
						restriction.setPropertyValue(res[1].trim());
						restriction.setCondition(">=");
						liresult.add(restriction);
					}
					else if(result[i].contains("<=")) {
						final String[] res = result[i].split(REGW);
						//final Restriction restriction = new Restriction(res[0].trim(),res[1].trim(),"<="); // Not the best case as we've setters already
						//is the best practice, but test cases are written like that
						  final Restriction restriction = new Restriction();
						restriction.setPropertyName(res[0].trim());
						restriction.setPropertyValue(res[1].trim());
						restriction.setCondition("<=");
						liresult.add(restriction);
					}
					else if(result[i].contains(">")) {
						final String[] res = result[i].split(REGW);
						//final Restriction restriction = new Restriction(res[0].trim(),res[1].trim(),">"); // Not the best case as we've setters already
						//is the best practice, but test cases are written like that
						  final Restriction restriction = new Restriction();
						restriction.setPropertyName(res[0].trim());
						restriction.setPropertyValue(res[1].trim());
						restriction.setCondition(">");
						liresult.add(restriction);
					}
					else if(result[i].contains("<")) {
						final String[] res = result[i].split(REGW);
						//final Restriction restriction = new Restriction(res[0].trim(),res[1].trim(),"<"); // Not the best case as we've setters already
						//is the best practice, but test cases are written like that
						  final Restriction restriction = new Restriction();
						restriction.setPropertyName(res[0].trim());
						restriction.setPropertyValue(res[1].trim());
						restriction.setCondition("<");
						liresult.add(restriction);
					}
					else if(result[i].contains("!=")) {
						final String[] res = result[i].split(REGW);
						//final Restriction restriction = new Restriction(res[0].trim(),res[1].trim(),"!="); // Not the best case as we've setters already
						//is the best practice, but test cases are written like that
						  final Restriction restriction = new Restriction();
						  restriction.setPropertyName(res[0].trim());
						  restriction.setPropertyValue(res[1].trim()); restriction.setCondition("!=");
						 liresult.add(restriction);
						 
					}
					else if(result[i].contains("=")) {
						final String[] res = result[i].split(REGW);
						//final Restriction restriction = new Restriction(res[0].trim(),res[1].trim(),"="); // Not the best case as we've setters already
						// is the best practice, but test cases are written like that
						  final Restriction restriction = new Restriction();
						  restriction.setPropertyName(res[0].trim());
						  restriction.setPropertyValue(res[1].trim()); restriction.setCondition("=");
						 liresult.add(restriction);
						 
					}

				}
			}
		}

		return liresult;

	}

	/*
	 * Extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * The query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 * @param queryString
	 * @return
	 */

	public List<String> getLogicalOperators(final String queryString) {
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
			//return strOperator; is containing an array of length 100, we need to put it in another array/List to make it work
			List<String> result=new ArrayList<String>();
			for(int i = 0;i < j;i++) {
				result.add(strOperator[i]);			
			}
			return result;
		}
		else {
			return null;
		}	
	}


	/*
	 * Extract the aggregate functions from the query. The presence of the aggregate
	 * functions can determined if we have either "min" or "max" or "sum" or "count"
	 * or "avg" followed by opening braces"(" after "select" clause in the query
	 * string. in case it is present, then we will have to extract the same. For
	 * each aggregate functions, we need to know the following: 1. type of aggregate
	 * function(min/max/count/sum/avg) 2. field on which the aggregate function is
	 * being applied.
	 * 
	 * Please note that more than one aggregate function can be present in a query.
	 * @param queryString
	 * @return
	 * 
	 */

	public List<AggregateFunction> getAggregateFunctions(final String queryString) {
		String strFrom = queryString.toLowerCase().split("from")[0].trim();
		String strSelect = strFrom.split("select")[1].trim();
		String[] strFieldsAndAggrfunc = strSelect.split(",");
		ArrayList<String> myAggrFuncList = new  ArrayList<String>();
		ArrayList<AggregateFunction> list = new  ArrayList<AggregateFunction>();
		for(int i = 0;i < strFieldsAndAggrfunc.length;i++) { // or we can do List strFieldsAndAggrfunc = getFields(queryString) and iterate the array
			if(strFieldsAndAggrfunc[i].contains("(")) {
				myAggrFuncList.add(strFieldsAndAggrfunc[i].trim());
			}
		}
		int listSize = myAggrFuncList.size();
		if(listSize == 0) {
			return null;
		}else {
			for(int i=0;i<listSize;i++) {
				String[] aggrFuncArray = myAggrFuncList.get(i).split("\\(|\\)");
				//AggregateFunction af = new AggregateFunction(aggrFuncArray[1], aggrFuncArray[0]);// Not the best case as we've setters already
				// is the best practice, but test cases are written like that
				  AggregateFunction af = new AggregateFunction();
				  af.setFunction(aggrFuncArray[0]); af.setField(aggrFuncArray[1]);
				 list.add(af);
				 
			}
			return list;
		}
	}

	public static boolean empty(final String s) {
		// Null-safe, short-circuit evaluation.
		return s == null || s.trim().isEmpty();
	}

}