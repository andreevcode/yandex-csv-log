package edu.yandex;

import java.util.Date;

// Template for LogValue
public class LogValue {
    Date time;
    String src_user;
    String src_ip;
    String src_port;
    String dest_user;
    String dest_ip;
    String dest_port;
    long input_byte;
    long output_byte;
    long interval;

    public String getSrc_user() {
        return src_user;
    }

    public Long getOutput_byte() {
        return output_byte;
    }

    public long getInterval() {
        return interval;
    }

    public Date getTime() {
        return time;
    }
}
