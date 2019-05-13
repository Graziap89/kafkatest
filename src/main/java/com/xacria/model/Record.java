package com.xacria.model;
import java.util.Date;

public class Record {
    public String title;
    public String date;
    public String value;


    public Record() {
    }

    public Record(String title, String date, String value) {
        this.title = title;
        this.date = date;
        this.value = value;
    }

    public static Record getRecordFromLog(String log){
        String[] splitted = log.split(",");
        return new Record(splitted[0], splitted[1], splitted[2]);
    }
}
