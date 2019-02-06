package com.stackroute.datamunger.query;

import java.util.*;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.stackroute.datamunger.query.parser.Restriction;

//This class contains methods to evaluate expressions
public class Filter {

	/*
	 * the evaluateExpression() method of this class is responsible for evaluating
	 * the expressions mentioned in the query. It has to be noted that the process
	 * of evaluating expressions will be different for different data types. there
	 * are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method
	 * should be able to evaluate all of them. Note: while evaluating string
	 * expressions, please handle uppercase and lowercase
	 * 
	 */

	// method to evaluate expression for eg: salary>20000

	// method containing implementation of equalTo operator

	// method containing implementation of greaterThan operator

	// method containing implementation of greaterThanOrEqualTo operator

	// method containing implementation of lessThan operator

	// method containing implementation of lessThanOrEqualTo operator
	public static Boolean evaluateExpression (List<Restriction> restriction,List<String> logicalOperators,String[] row,Header header,RowDataTypeDefinitions rowDataType) {
		//JexlEngine engine= new JexlBuilder().create();
		String[] javaLogics=null;

		if(logicalOperators!=null) {
			if((logicalOperators.size()>1 ) && (logicalOperators.get(logicalOperators.size()-1)).equals("and") ) {
				Collections.reverse(logicalOperators);
				Collections.reverse(restriction);

			}
			javaLogics=new String[logicalOperators.size()];


			for(int i=0;i<logicalOperators.size();i++) {
				if(logicalOperators.get(i).equals("and")) {
					javaLogics[i]="&&";
				}
				else {
					javaLogics[i]="||";
				}
			}

		}


		String expression=null;
		for(int i=0;i<restriction.size();i++) {
			String property=restriction.get(i).getPropertyName();
			String op=restriction.get(i).getCondition();
			if(op=="=") {
				op="==";
			}
			String value=restriction.get(i).getPropertyValue();
			String propertyVal=row[(header.get(property))-1];
			Boolean isString=rowDataType.get((header.get(property))).equals("java.lang.String");
			if(i==0) {
				if(isString) {
					expression= "(\""+propertyVal.toLowerCase()+"\""+" "+op+" "+"\""+value.toLowerCase()+"\")";
				}
				else {
					expression= "("+propertyVal+" "+op+" "+value+")";
				}
			}
			else  {
				String logic=javaLogics[i-1];
				if(isString) {
					expression=expression+" "+logic+" "+"(\""+propertyVal.toLowerCase()+"\""+" "+op+" "+"\""+value.toLowerCase()+"\")";

				}
				else {
					expression=expression+" "+logic+" ("+propertyVal+" "+op+" "+value+")";
				}

			}
			if(((i+1)%2)==0) {
				expression="("+expression+")";
			}

		}
		//	System.out.println("expression is:"+expression);
		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine engine = mgr.getEngineByName("JavaScript");
		try {
			return (Boolean) engine.eval(expression);
		} catch (ScriptException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}


	}

}
