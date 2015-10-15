package com.ccc.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author Tom
 */
public class UserQDataWritable implements Writable {

    private Long uid;
    private Long[] querys;
    private Integer[] submits;

    public UserQDataWritable(Long uid, Long[] querys, Integer[] clicks) {
        this.uid = uid;
        this.querys = querys;
        this.submits = clicks;
    }

    public UserQDataWritable() {

    }

    public void set(Long uid, Long[] querys,Integer[] clicks) {
        this.uid = uid;
        this.querys = querys;
        this.submits = clicks;
    }

    public Long[] getQueries() {
        return this.querys;
    }

    public Integer[] getClicks() {
        return submits;
    }

    public Long getUID() {
        return uid;
    }

    @Override
    public void write(DataOutput d) throws IOException {
        d.writeInt(querys.length);
        for (Integer i : submits) {
            d.writeInt(i);
        }
        d.writeLong(uid);
        for (Long i : querys) {
            d.writeLong(i);
        }

    }

    @Override
    public void readFields(DataInput di) throws IOException {
        int size = di.readInt();
        querys = new Long[size];
        submits = new Integer[size];   
        for (int i = 0; i < size; i++) {
            submits[i] = di.readInt();
        }
        uid=di.readLong();
        for (int i = 0; i < size; i++) {
            this.querys[i] = di.readLong();
        }


    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(); //To change body of generated methods, choose Tools | Templates.
        sb.append(querys[0]);
        for (int i = 1; i < querys.length; i++) {
            sb.append(" ").append(querys[i]);

        }

        return sb.toString();
    }

}
