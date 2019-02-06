package com.stackroute.datamunger.query.parser;

import java.util.*;

public class QueryParser_New {

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
	public String getFileName(final String queryString) {
		final StringBuffer output = new StringBuffer();
		final String [] query = this.getSplitStrings(queryString);
		for(int i=0;i<query.length;i++) {
			if(query[i].endsWith(".csv")){
				output.append(query[i]);
			}	
		}
		return output.toString();
	}

	/*
	 * Extract the baseQuery from the query.This method is used to extract the
	 * baseQuery from the query string. BaseQuery contains from the beginning of the
	 * query till the where clause
	 * @param queryString
	 * @return
	 */

	public String getBaseQuery(final String queryString) {
		final StringBuffer output = new StringBuffer();
		final String [] query = this.getSplitStrings(queryString);
		for(int i=0;i<query.length;i++) {
			if(query[i].equals(WHERE) || query[i].equals(GROUP) || query[i].equals(ORDER) && query[i+1].equals(GRORBY)){
				break;
			}
			output.append(query[i]);
			output.append(' ');
		}
		return output.toString().trim();
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
		final StringBuffer output = new StringBuffer();
		String[] result=null;
		final List<String> liresult=new ArrayList<String>();
		final String [] query = this.getSplitStrings(queryString);
		for(int i=0;i<query.length;i++) {
			if(query[i].equals(ORDER) && query[i+1].equals(GRORBY)) {
				for(int j=i+2;j<query.length;j++) {
					if(query[j].contains(";")) {
						break;
					}
					output.append(query[j]);
					output.append(' ');
				}
				result= output.toString().trim().split(" |,");
				for (final String string : result) {
					liresult.add(string);
				}
			}
		}
		return liresult;
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
		final StringBuffer output = new StringBuffer();
		String result[]=null;
		List<String> liresult=null;
		final String [] query = this.getSplitStrings(queryString);
		for(int i=0;i<query.length;i++) {
			if(query[i].equals(GROUP) && query[i+1].equals(GRORBY)) {
				for(int j=i+2;j<query.length;j++) {
					if(query[j].contains(";") || (query[j].contains(ORDER) && query[j+1].contains(GRORBY))) {
						break;
					}
					output.append(query[j]);
					output.append(' ');
				}
				result= output.toString().trim().split(" |,");
				liresult=new ArrayList<String>();
				for (final String string : result) {
					liresult.add(string);
				}
			}
		}
		return liresult;
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
		final StringBuffer output = new StringBuffer();
		String result[]=null;
		final String [] query = this.getBaseQuery(queryString).split(" ");
		final List<String> liresult=new ArrayList<String>();
		for(int i=0;i<query.length;i++) {
			if(query[i].contains("select")){
				continue;
			}
			else if(query[i].contains("from")){
				break;
			}
			output.append(query[i]);
		}
		result=output.toString().trim().split(",");
		for (final String string : result) {
			liresult.add(string);
		}
		return liresult;
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
		final String [] query = this.getSplitStrings(queryString);
		final StringBuffer output= new StringBuffer();
		String result[]=null;
		List<String> liresult=null;
		if(queryString.contains(" and ") || queryString.contains(" or ") && queryString.contains(" where ")) {
			for(int i=0;i<query.length;i++) {
				if(query[i].contains(WHERE)){
					for(int j=i+1;j<query.length;j++) {
						if(query[j].equals(ORDER) || query[j].equals(GROUP) && query[j+1].equals(GRORBY)){
							break;
						}
						if(query[j].equals("and") || query[j].equals("or")) {
							output.append(query[j]);
							output.append(' ');
						}
					}
					liresult=new ArrayList<String>();
					result= output.toString().split(" ");
					for (final String string : result) {
						liresult.add(string);
					}
					break;
				}
			}
		}
		return liresult;
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
		final StringBuffer output = new StringBuffer();
		String result[]=null;
		final String [] query = this.getBaseQuery(queryString).split(" ");
		final List<AggregateFunction> liresult=new ArrayList<AggregateFunction>();
		for(int i=0;i<query.length;i++) {
			if(query[i].contains("select")){
				continue;
			}
			else if(query[i].contains("from")){
				break;
			}
			output.append(query[i]);
		}
		result=output.toString().trim().split(",");

		if(!result[0].equals("*")) {
			for(int i=0;i<result.length;i++) {
				if(result[i].startsWith("max(")  || result[i].startsWith("min(") ||  result[i].startsWith("count(") || result[i].startsWith("avg(") || result[i].startsWith("sum") && result[i].endsWith(")")) {
					final String sep[]=result[i].split("[)|(]");
					//final AggregateFunction aggregateFunction = new AggregateFunction(sep[1],sep[0]); // Lets do it with setters
					final AggregateFunction aggregateFunction = new AggregateFunction();
					aggregateFunction.setFunction(sep[0]);
					aggregateFunction.setField(sep[1]);
					liresult.add(aggregateFunction);
				}			
			}
		}
		return liresult;
	}

}