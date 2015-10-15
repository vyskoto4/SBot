package com.ccc.utilities;

import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author Tom
 */
public class FilesUtils {

    public static void copySubdirectories(Path sourcePath, Path targetDirPath, FileSystem hdfs) throws IOException {

        for (FileStatus fs : hdfs.listStatus(sourcePath)) {
            if (fs.isDirectory()) {
                Path origPath = fs.getPath();
                String fname = origPath.getName();
                fname = fname.substring(fname.lastIndexOf("/") + 1);
                Path targetPath = new Path(targetDirPath, fname);
                hdfs.rename(origPath, targetPath);
            }
        }
    }

}
