package com.stackroute.datamunger;

public class DataMungerApp{
	public static void main(String[] args) {
		String queryString ="select city,winner,player_match from ipl.csv where season > 2014 and city ='Bangalore' or city ='Delhi'";
		
		String str = queryString.toLowerCase();
		String[] strAndOr = null;
		String[] strOperator = null;
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
		}
		else {
			strOperator = null;
		}	
		System.out.println(strOperator);
			
	}
}