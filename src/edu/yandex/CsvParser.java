package edu.yandex;

import com.sun.jdi.LongValue;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvParser {

    private static final String COMMA = ",";

    public List<LogValue> processInputFile(String inputFilePath) {
        List<LogValue> inputList = new ArrayList<LogValue>();
        try{
            File inputF = new File(inputFilePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            inputList = br.lines()
                    .skip(1)            // skip the header of the csv
                    .map(getLogValue)
                    .collect(Collectors.toList());
            br.close();
            //} catch (FileNotFoundException | IOException e) {
        } catch (IOException e) {
            System.out.println("Error during readig file");
        }
        return inputList ;
    }

    //Maping line to logValue
    private Function<String, LogValue> getLogValue = (line) -> {
        String[] st = line.split(COMMA);
        LogValue value = new LogValue();
        value._time=st[0];
        value.src_user=st[1];
        value.src_ip=st[2];
        value.src_port=st[3];
        value.dest_user=st[4];
        value.dest_ip=st[5];
        value.dest_port=st[6];
        value.input_byte=Long.parseLong(st[7]);
        value.output_byte=Long.parseLong(st[8]);
        return value;
    };

    //Find most active users
    public Map<String,Long> findActiveUsers(List<LogValue> logValueList){
        Map<String, Long> users =  logValueList.stream()
                .collect(Collectors.groupingBy(LogValue::getSrc_user, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted( (m1,m2)->Long.compare(m2.getValue(), m1.getValue()))
                .limit(6)
                .collect(Collectors.toMap(m->m.getKey(), m->m.getValue()));
        return users;
    }

    // Find users with most output
    public Map<String,Long> findMostOutputUsers(List<LogValue> logValueList){
        Map<String, Long> users =  logValueList.stream()
                .collect(Collectors.groupingBy(LogValue::getSrc_user, Collectors.summingLong(LogValue::getOutput_byte)))
                .entrySet()
                .stream()
                .sorted( (m1,m2)->Long.compare(m2.getValue(), m1.getValue()))
                .limit(6)
                .collect(Collectors.toMap(m->m.getKey(), m->m.getValue()));
        return users;
    }
}
