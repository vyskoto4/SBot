package com.ccc.utilities;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Tom
 *
 *
 */
public class StringBytesOutputFormat<K, V> extends FileOutputFormat<K, V> {

    protected static class LineRecordWriter<K, V>
            extends RecordWriter<K, V> {

        protected DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        /**
         * Write String data to the byte output stream
         *
         * @param key is present only because of inheritance, is ignored
         * @param value represents the String that should be written to output
         * stream, non String values are ignored
         * @throws IOException
         */
        @Override
        public synchronized void write(K key, V value)
                throws IOException {

            if (value instanceof byte[] && value != null) {
                out.write((byte[]) value);
            }
        }

        @Override
        public synchronized
                void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }

    @Override
    public RecordWriter<K, V>
            getRecordWriter(TaskAttemptContext job
            ) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        String extension = "";
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new LineRecordWriter<K, V>(fileOut);

    }
}
