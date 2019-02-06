package com.stackroute.datamunger.query.parser;

import java.util.*;

/* 
 * This class will contain the elements of the parsed Query String such as conditions,
 * logical operators,aggregate functions, file name, fields group by fields, order by
 * fields, Query Type
 * */
public class QueryParameter {

	private String file;
	private String baseQuery;
	private List<Restriction> restrictions = new ArrayList<Restriction>();
	private List<String> fields ;
	private List<String> logicalOperators;
	private List<String> orderByFields;
	private List<String> groupByFields;
	private List<AggregateFunction> aggregateFunctions;
	private String QUERY_TYPE;



	public String getFileName() {
		return file;
	}

	public void setFileName(final String file) {
		this.file=file;
	}

	public String getBaseQuery() {
		return baseQuery;
	}

	public void setBaseQuery(final String baseQuery) {
		this.baseQuery=baseQuery;
	}

	public List<Restriction> getRestrictions() {
		return restrictions;
	}

	public void setRestrictions(final List<Restriction> restrictions) {
		this.restrictions=restrictions;
	}

	public List<String> getLogicalOperators() {
		return logicalOperators;
	}

	public void setLogicalOperators(final List<String> list) {
		this.logicalOperators=list;
	}	


	public List<String> getFields() {
		return fields;
	}

	public void setFields(final List<String> list) {
		this.fields=list;
	}

	public List<AggregateFunction> getAggregateFunctions() {
		return aggregateFunctions;
	}

	public void setAggregateFunctions(final List<AggregateFunction> list) {
		this.aggregateFunctions=list;
	}

	public List<String> getGroupByFields() {
		return groupByFields;
	}

	public void setGroupByFields(final List<String> list) {
		this.groupByFields=list;
	}

	public List<String> getOrderByFields() {
		return orderByFields;
	}

	public void setOrderByFields(final List<String> list) {
		this.orderByFields=list;
	}

	public String getQUERY_TYPE() {
		return QUERY_TYPE;
	}

	public void setQUERY_TYPE(final String QUERY_TYPE) {
		this.QUERY_TYPE=QUERY_TYPE;
	}


}