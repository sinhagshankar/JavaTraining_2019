package com.stackroute.datamunger.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.stackroute.datamunger.query.parser.Restriction;

//This class contains methods to evaluate expressions
public class Filter {
	RowDataTypeDefinitions rowDataTypeMap;
	String dataType;
	boolean bool;
	String name;
	
	/* 
	 * The evaluateExpression() method of this class is responsible for evaluating 
	 * the expressions mentioned in the query. It has to be noted that the process 
	 * of evaluating expressions will be different for different data types. there 
	 * are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method 
	 * should be able to evaluate all of them. 
	 * Note: while evaluating string expressions, please handle uppercase and lowercase 
	 * 
	 */
	
	
	
public boolean evaluateExpression(Restriction restriction, String fieldValue, String dataType)  {
	if(restriction.getCondition().equals("="))
		return equalTo(fieldValue, restriction.getPropertyValue(), dataType);
	else if(restriction.getCondition().matches("!="))
		return notequalTo(fieldValue, restriction.getPropertyValue(), dataType);
	else if(restriction.getCondition().equals(">"))
		return greaterThan(fieldValue, restriction.getPropertyValue(), dataType);
	else if(restriction.getCondition().equals(">="))
		return greaterThanequalTo(fieldValue, restriction.getPropertyValue(), dataType);
	else if(restriction.getCondition().equals("<"))
		return lessThan(fieldValue, restriction.getPropertyValue(), dataType);
	else
		return lessThanequalTo(fieldValue, restriction.getPropertyValue(), dataType);
}

private String getDateFormat(String date) {
	String format = "";
	if(date.matches("^[0-9]{2}/[0-9]{2}/[0-9]{4}$"))
		format = "dd/mm/yyyy";
	else if(date.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"))
		format = "yyyy-mm-dd";
	else if(date.matches("^[0-9]{2}-[a-z]{3}-[0-9]{2}$"))
		format = "dd-mon-yy";
	else if(date.matches("^[0-9]{2}-[a-z]{3}-[0-9]{4}$"))
		format ="dd-mon-yyyy";
	else if(date.matches("^[0-9]{2}-[a-z]{3,9}-[0-9]{2}$"))
		format = "dd-month-yy";
	else 
		format ="dd-month-yyyy";
	return format;
}
	
	
	//Method containing implementation of equalTo operator
	
private boolean equalTo(String name, String value, String dataType){
	if(dataType.equals("java.lang.Integer")) {
		return Integer.parseInt(name)==Integer.parseInt(value);
	} else if(dataType.equals("java.lang.Double")) {
		return Double.parseDouble(name)==Double.parseDouble(value);
	} else if(dataType.equals("java.util.Date")) {
		 SimpleDateFormat formatter = new SimpleDateFormat(getDateFormat(name));
		 try {
			if(formatter.parse(name).compareTo(formatter.parse(value))==0)
				 return true;
			 else
				 return false;
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		}
	} else if(dataType.equals("java.util.Object"))
		return false;
	else {
		if(name.compareTo(value)==0)
			return true;
		else
			return false;
	}
}
	
	
	//Method containing implementation of notEqualTo operator
	
private boolean notequalTo(String name, String value, String dataType)  {
	return !equalTo(name, value, dataType);

}
	
	//Method containing implementation of greaterThan operator
	
private boolean greaterThan(String name, String value, String dataType)  {
	
	if(dataType.equals("java.lang.Integer")) {
		return Integer.parseInt(name)>Integer.parseInt(value);
	} else if(dataType.equals("java.lang.Double")) {
		return Double.parseDouble(name.toLowerCase())>Double.parseDouble(value.toLowerCase());
	} else if(dataType.equals("java.util.Date")) {
		 SimpleDateFormat formatter = new SimpleDateFormat(getDateFormat(name));
		 try {
			if(formatter.parse(name).compareTo(formatter.parse(value))>0)
				 return true;
			 else
				 return false;
		} catch (ParseException e) {
			e.printStackTrace();
			return false;
		}
	} else if(dataType.equals("java.util.Object"))
		return false;
	else {
		if(name.compareTo(value)>0)
			return true;
		else
			return false;
	}
}
	
	
	
	
	
	//Method containing implementation of greaterThanOrEqualTo operator
	
private boolean greaterThanequalTo(String name, String value, String dataType) {
	
	return equalTo(name, value, dataType)|greaterThan(name, value, dataType);
}
	
	
	
	
	//Method containing implementation of lessThan operator
	  
	
private boolean lessThan(String name, String value, String dataType) {
	return !greaterThanequalTo(name, value, dataType);
}
	
	//Method containing implementation of lessThanOrEqualTo operator
	
private boolean lessThanequalTo(String name, String value, String dataType) {
	return equalTo(name, value, dataType)|lessThan(name, value, dataType);
}
}