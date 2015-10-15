package com.ccc.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author Tom
 */
public class LongPairWritable implements WritableComparable<LongPairWritable> {

    private Long first;
    private Long second;

    public LongPairWritable() {
    }

    public LongPairWritable(long first, long sec) {
        this.first = first;
        this.second = sec;
    }

    public Long getFirst() {
        return first;
    }

    public Long getSecond() {
        return second;
    }

    public void setLongs(long first, long sec) {
        this.first = first;
        this.second = sec;
    }

    public void setLongs(Long first, Long sec) {
        this.first = first;
        this.second = sec;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public int compareTo(LongPairWritable o) {
        int res = Long.compare(first, o.getFirst());
        if (res == 0) {
            res = Long.compare(second, o.getSecond());
        }
        return res;

    }

    public void setLongsFromHexString(String hex) {
        this.first = Long.parseLong(hex.substring(0, 8), 16);
        this.second = Long.parseLong(hex.substring(8), 16);
        Long.parseLong(hex.substring(8), 16);
    }

    @Override
    public String toString() {
        return first + " " + second;
    }

    @Override
    public int hashCode() {
        final int prime = 13;
        int result = 1;
        result = prime * result + ((first == null) ? 0 : first.hashCode());
        result = prime * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

}
