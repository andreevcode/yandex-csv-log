package edu.yandex;

import java.lang.reflect.Field;

public class LogValue {
    String _time;
    String src_user;
    String src_ip;
    String src_port;
    String dest_user;
    String dest_ip;
    String dest_port;
    long input_byte;
    long output_byte;

    public String getSrc_user() {
        return src_user;
    }

    public Long getOutput_byte() {
        return output_byte;
    }


}
