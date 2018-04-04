package edu.yandex;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class CsvParser {

    private static final String COMMA = ",";
    private static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    //CSV file parsing
    public static List<LogValue> parseCsv(String inputFilePath) {
        List<LogValue> inputList = new ArrayList<LogValue>();
        try{
            File inputF = new File(inputFilePath);
            InputStream inputFS = new FileInputStream(inputF);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
            inputList = br.lines()
                    .skip(1)            // skip the header of the csv
                    .map(getLogValue)
                    .sorted((m1, m2) -> m1.time.compareTo(m2.time) ) //Sorting by Date
                    .collect(Collectors.toList());
            br.close();
        } catch (IOException e) {
            System.out.println("Error during readig file");
        }
        return inputList ;
    }

    //Mapping line to logValue
    private static Function<String, LogValue> getLogValue = (line) -> {
        String[] st = line.split(COMMA);
        LogValue value = new LogValue();
        //Prepare string to format as Date
        String dateString = st[0].substring(1,st[0].length()-1);
        value.time=getDate(dateString);
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

    // Get Date from String with SIMPLE_DATE_FORMAT
    private static Date getDate(String st){
        Date date = new Date();
        try {
            date = SIMPLE_DATE_FORMAT.parse(st);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    //Find most active users
    public static Map<String,Long> findActiveUsers(List<LogValue> logValueList){
        Map<String, Long> users =  logValueList.stream()
                .collect(Collectors.groupingBy(LogValue::getSrc_user, Collectors.counting()))
                .entrySet()
                .stream()
                .sorted( (m1,m2)->Long.compare(m2.getValue(), m1.getValue()))
                .limit(5)
                .collect(Collectors.toMap(m->m.getKey(), m->m.getValue()));
        return users;
    }

    // Find users with most output
    public static Map<String,Long> findMostOutputUsers(List<LogValue> logValueList){
        Map<String, Long> users =  logValueList.stream()
                .collect(Collectors.groupingBy(LogValue::getSrc_user, Collectors.summingLong(LogValue::getOutput_byte)))
                .entrySet()
                .stream()
                .sorted( (m1,m2)->Long.compare(m2.getValue(), m1.getValue()))
                .limit(5)
                .collect(Collectors.toMap(m->m.getKey(), m->m.getValue()));
        return users;
    }

    //Find interval between current and previous Logvalue in the list by the time field.
    private static BiFunction<List<LogValue>,LogValue,Long> getInterval = (list, value) ->{
        int i = list.indexOf(value);
        long interval = value.time.getTime()-list.get(i-1).time.getTime();
        return interval;
    };

    //Custom Distinct Predicate
    public static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    // Get Map <String, List<Long>> of intervals grouped by Users. Date stamps are distinct.
    // It's needed to get Statistics through LongStream.summaryStatistics
    public static Function<List<LogValue>,Map<String,List<Long>>> getDistinctIntervalsMap= list->{
        //Create Map <String, List<LogValue>> in which LogValues grouped by src_user field.
       Map<String,List<LogValue>> map = list.stream()
                .collect(Collectors.groupingBy(LogValue::getSrc_user));

       //Mapping current map to Map<String,<LogValue>> with distinct Dates
       Map<String,List<LogValue>> distinctMap = map.entrySet()
               .stream()
               .map(elem->{
                   List <LogValue> iList= elem.getValue().stream()
                           .filter(distinctByKey(LogValue::getTime))
                           .collect(Collectors.toList());
                   Map.Entry<String, List<LogValue>> mEntry = new MyEntry<String, List<LogValue>>(elem.getKey(),iList);
                   return mEntry;
               })
               .collect(Collectors.toMap(m->m.getKey(),m->m.getValue()));

        //Filling distinctMap with Intervals in
        distinctMap.forEach ((k,v) -> v.stream()
                        .skip(1)
                        .forEach (value -> value.interval=getInterval.apply(v,value))
        );

        //Maping distinctMap to returnMap Map<String,:ist<Long>> with only interval values.
        //Stream interval values to LongStream and get SummaryStatistics
        //Print Flag of periodic, Min, Max, Avg intervals at the same time
        Map<String,List<Long>> returnMap = distinctMap.entrySet()
                .stream()
                //ниже тестовая строка для сортировки по кол-ву соединений
                .sorted( (m1,m2)->Long.compare(m2.getValue().size(), m1.getValue().size()))
                .map(elem->{
                    List <Long> iList= elem.getValue().stream()
                            .map(LogValue::getInterval)
                            .collect(Collectors.toList());
                    Map.Entry<String, List<Long>> mEntry = new MyEntry<String, List<Long>>(elem.getKey(),iList);
                    LongStream longStream = iList.stream()
                                                    .skip(1)
                                                    .mapToLong(x->x);
                    LongSummaryStatistics stat = longStream.summaryStatistics();
                    DecimalFormat df2 = new DecimalFormat(".##");
                    String flag=(stat.getMax()==stat.getMin()) ? "PROB" : "NO  ";
                    System.out.println(flag+ " Average: "+ df2.format(stat.getAverage())+
                            " Max: "+ df2.format(stat.getMax()) +
                            " Min: "+ df2.format(stat.getMin()) +
                            " Count: "+ stat.getCount());
                    return mEntry;
                })
                .collect(Collectors.toMap(m->m.getKey(),m->m.getValue()));
        return returnMap;
    };

    //Custom Map<k,v>Entry Class
    private static class MyEntry<K, V> implements Map.Entry<K, V> {
        private final K key;
        private V value;
        public MyEntry(final K key) {
            this.key = key;
        }
        public MyEntry(final K key, final V value) {
            this.key = key;
            this.value = value;
        }
        public K getKey() {
            return key;
        }
        public V getValue() {
            return value;
        }
        public V setValue(final V value) {
            final V oldValue = this.value;
            this.value = value;
            return oldValue;
        }
    }

}
