package com.stackroute.datamunger.query.parser.Util;

import java.util.ArrayList;
import java.util.List;

public class DataMungerUtil {

	public static void main(String[] args) {
		String queryString ="select winner,season,team2 from ipl.csv where season > 2014 group by winner order by team1";
		String str = queryString.toLowerCase();
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
		}
		for (String string : strGroupByFields) {
			result.add(string);
		}
		
		System.out.println(result);

	}
}