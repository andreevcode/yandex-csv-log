package edu.yandex;

import com.sun.jdi.LongValue;
import javafx.scene.input.KeyCode;

//import javax.xml.bind.annotation.adapters.CollapsedStringAdapter;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

//import static sun.tools.java.Constants.COMMA;

public class Main {



    public static void main(String[] args) {
	    System.out.println(getCurrentDate());

        CsvParser csvParser = new CsvParser();

        List<LogValue> logValueList= csvParser.processInputFile("C:\\TestPath\\shkib.csv");
        System.out.println("\n" + getCurrentDate());
        //HashMap with mostActive users
        Map<String,Long> activeUsers = csvParser.findActiveUsers(logValueList);
        Map<String,Long> mostOutputUsers = csvParser.findMostOutputUsers(logValueList);

        activeUsers.forEach((k,v)->System.out.println(k + " " + v));
        System.out.println("\n" + getCurrentDate());
        mostOutputUsers.forEach((k,v)->System.out.println(k + " " + v));
        System.out.println("\n" + getCurrentDate());
    }

    private static void printField(String st){
        System.out.print(st+" ");
    }

    private static String getCurrentDate(){
        SimpleDateFormat sdDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Date now = new Date();
        String strDate = sdDate.format(now);
        return  strDate;
    }



}
