package com.stackroute.datamunger.query.parser;

/* This class is used for storing name of field, aggregate function for 
 * each aggregate function
 * */
public class AggregateFunction {


	public	String field=null;
	public	String function=null;

	// Write logic for constructor
	public AggregateFunction() {}
	/*
	 * public AggregateFunction(final String field,final String function) {
	 * this.field=field; this.function=function; }
	 */
	
	public void setField(String field) {
		this.field = field;
	}

	public void setFunction(String function) {
		this.function = function;
	}

	public String getField() {
		return field;
	}

	public String getFunction() {
		return function;
	}
}
