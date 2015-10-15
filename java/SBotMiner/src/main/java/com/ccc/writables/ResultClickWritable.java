package com.ccc.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Tom
 */
public class ResultClickWritable implements Writable {

    private Long uidHash, urlHash;
    private Double timestamp;

    public ResultClickWritable() {
    }

    public void set(Long uidHash, Long urlHash, Double timestamp) {
        this.uidHash = uidHash;
        this.urlHash = urlHash;
        this.timestamp = timestamp;
    }

    public Long getUidHash() {
        return uidHash;
    }

    public void setUidHash(Long uidHash) {
        this.uidHash = uidHash;
    }

    public Long getUrlHash() {
        return urlHash;
    }

    public void setUrlHash(Long urlHash) {
        this.urlHash = urlHash;
    }

    public Double getTime() {
        return timestamp;
    }

    public void setTime(Double time) {
        this.timestamp = time;
    }

    @Override
    public void write(DataOutput d) throws IOException {

        d.writeLong(uidHash);
        d.writeLong(urlHash);
        d.writeDouble(timestamp);
    }

    @Override
    public void readFields(DataInput di) throws IOException {

        uidHash = di.readLong();
        urlHash = di.readLong();
        timestamp = di.readDouble();
    }

}
