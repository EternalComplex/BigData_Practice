package com.ct.producer.io;

import com.ct.common.bean.DataOut;

import java.io.*;

/**
 * 本地数据文件输出
 */
public class LocalFileDataOut implements DataOut {

    private PrintWriter writer = null;

    public LocalFileDataOut(String path) {
        setPath(path);
    }

    public void setPath(String path) {
        try {
            this.writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(path), "utf-8"));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(Object data) throws IOException {
        write(data.toString());
    }

    public void write(String data) throws IOException {
        writer.println(data);
        System.out.println(data);
        writer.flush();
    }

    public void close() throws IOException {
        if (writer != null) writer.close();
    }
}
