package com.ccc.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Tom
 */
public class UserStatsWritable implements Writable {

    private boolean js, limit, rss;
    private String ip, ua;
    private Long creationDate;
    private Double requestDate;
    private String query;

    public UserStatsWritable(String query,boolean js, boolean limit, boolean rss, String ip, String ua, Long creationDate, Double requestDate) {
        this.query=query;
        this.js = js;
        this.limit = limit;
        this.rss = rss;
        this.ip = ip;
        this.ua = ua;
        this.creationDate = creationDate;
        this.requestDate = requestDate;
    }

    public void set(String query,boolean js, boolean limit, boolean rss, String ip, String ua, Long creationDate, Double requestDate) {
        this.query=query;
        this.js = js;
        this.limit = limit;
        this.rss = rss;
        this.ip = ip;
        this.ua = ua;
        this.creationDate = creationDate;
        this.requestDate = requestDate;
    }

    public UserStatsWritable() {
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public boolean isJs() {
        return js;
    }

    public void setJs(boolean js) {
        this.js = js;
    }

    public boolean isLimit() {
        return limit;
    }

    public void setLimit(boolean limit) {
        this.limit = limit;
    }

    public boolean isRss() {
        return rss;
    }

    public void setRss(boolean rss) {
        this.rss = rss;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUa() {
        return ua;
    }

    public void setUa(String ua) {
        this.ua = ua;
    }

    public Long getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Long creationDate) {
        this.creationDate = creationDate;
    }

    public Double getRequestDate() {
        return requestDate;
    }

    public void setRequestDate(Double requestDate) {
        this.requestDate = requestDate;
    }
    

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(query);
        d.writeBoolean(js);
        d.writeBoolean(limit);
        d.writeBoolean(rss);

        d.writeUTF(ip);
        d.writeUTF(ua);

        d.writeLong(creationDate);
        d.writeDouble(requestDate);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        query=di.readUTF();
        js = di.readBoolean();
        limit = di.readBoolean();
        rss = di.readBoolean();

        ip = di.readUTF();
        ua = di.readUTF();
        creationDate = di.readLong();
        requestDate=di.readDouble();
    }

}
