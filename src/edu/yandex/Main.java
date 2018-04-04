package edu.yandex;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.*;

public class Main {

    // Parsing CSV and making some tests
    public static void main(String[] args) {
	    System.out.println(getCurrentDate()+" Parsing CSV started");

	    // Parsing CSV
        List<LogValue> logValueList= CsvParser.parseCsv("C:\\TestPath\\shkib.csv");
        System.out.println(getCurrentDate()+" CSV was parsed");

        //Getting Map of most active users (by the count of connections)
        Map<String,Long> activeUsers = CsvParser.findActiveUsers(logValueList);
        System.out.println("\n" + getCurrentDate()+" Most active users founded");
        activeUsers.forEach((k,v)->System.out.println(k + " " + v));

        //Getting Map with users which have most output traffic
        Map<String,Long> mostOutputUsers = CsvParser.findMostOutputUsers(logValueList);
        System.out.println("\n" + getCurrentDate()+" Src_users with most traffic founded");
        mostOutputUsers.forEach((k,v)->System.out.println(k + " " + v));

        //Getting Map with src_users and their Date intervals between distinct connections
        //Printing statistics at the same time
        Map<String,List<Long>> intervals2Map = CsvParser.getDistinctIntervalsMap.apply(logValueList);
        System.out.println(getCurrentDate()+" A map of Distinct intervals by users was created");
    }

    //Get current Date time stamp
    private static String getCurrentDate(){
        SimpleDateFormat sdDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        String strDate = sdDate.format(now);
        return  strDate;
    }
}
