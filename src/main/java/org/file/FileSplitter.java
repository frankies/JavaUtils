/******************************************************************************/
/* SYSTEM     :                                                      */
/*                                                                            */
/* SUBSYSTEM  :                                                            */
/******************************************************************************/
package org.file;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class FileSplitter {

    private long blockSize;

    private long blockNum;

    private File srcFile;

    private File targetDir;

    private String prefix = "test-";

    private String suffix = ".part";

    /**
    *
    */
    public FileSplitter(File f, long blockSize, String targetDir) {

        this.srcFile = this.checkFileExists(f, false);
        this.blockSize = blockSize;
        this.targetDir = this.checkFileExists(targetDir, true);
        long len = f.length();
        this.blockNum = (len % blockSize) == 0 ? (len / blockSize) : (len / blockSize) + 1;
    }

    /**
     *
     */
    public FileSplitter(File f, int blockNumber, String targetDir) {

        long len = f.length();
        long lnum = new Long(blockNumber).longValue();

        this.srcFile = this.checkFileExists(f, false);
        this.blockSize = len / lnum;
        this.blockNum = blockNumber;
        this.targetDir = this.checkFileExists(targetDir, true);
    }

    /**
     * @param f
     * @param isDir
     * @return
     */
    private File checkFileExists(String path, boolean isDir) {

        if (path != null && path.length() > 0) {
            File p = new File(path);
            return checkFileExists(p, isDir);
        } else {
            throw new IllegalArgumentException("Input path is empty.");
        }
    }

    private File checkFileExists(File p, boolean isDir) {

        if (!p.exists()) {
            throw new IllegalArgumentException("Input path is not found.");
        }

        if (isDir && !p.isDirectory()) {
            throw new IllegalArgumentException("Input path must be a directory");
        } else if (!isDir && p.isDirectory()) {
            throw new IllegalArgumentException("Input path must not be a directory");
        }

        return p;
    }

    private String getPrefix() {
        return prefix;
    }

    private String getSuffix() {
        return suffix;
    }

    /**
     * @return
     */
    public File[] start() {

        FileChannel fc = null;
        List<File> fls = new ArrayList<>();
        try {
            // fc = new FileInputStream(this.srcFile).getChannel();
//            fc = FileChannel.open(Paths.get(this.srcFile.getAbsolutePath()), StandardOpenOption.READ);
            long len = this.srcFile.length();
            long position = 0;
            int middleLen = Long.valueOf(this.blockNum).toString().length();
            CountDownLatch cdl = new CountDownLatch(Long.valueOf(this.blockNum).intValue());
            int x = 1;
            long bz = -1;

            fc = new RandomAccessFile(this.srcFile, "r").getChannel();
            while (position <= len-1) {
                bz = Math.min(len - position, this.blockSize);
                log(x + ":" + position + "/" + bz);
                MappedByteBuffer buff = fc.map(FileChannel.MapMode.READ_ONLY, position, bz);
                log("Load content: " + buff.hasRemaining() + ", " + buff.limit());
                String fpath = String.format("%s%s%s", this.getPrefix(), getMiddle(middleLen, x), this.getSuffix());
                Worker w = new Worker(buff, fpath, cdl, bz);
                w.start();
                fls.add(w.getFile());
                x++;
                position += bz;
            };
            cdl.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeQuietly(fc);
        }

        return fls.toArray(new File[fls.size()]);
    }

    /**
     * @param len
     * @param index
     * @return
     */
    private String getMiddle(int len, int index) {

        return paddingLeft("" + index, len);
    }

    /**
     * @param c
     * @param len
     * @return
     */
    private String paddingLeft(String c, int len) {

        int fc = len - c.length();
        if (fc == 0) {
            return c;
        }
        String s = "";
        while (fc > 0) {
            s += "0";
            fc--;
        }
        return s + c;
    }

    private class Worker extends Thread {

        MappedByteBuffer buff;
        File targetFile;
        CountDownLatch latch;
        long expectSize;

        boolean isOk;

        /**
         * @param size TODO
         *
         */
        public Worker(MappedByteBuffer buff, String fileName, CountDownLatch latch, long size) {

            this.buff = buff;
            this.targetFile = new File(targetDir, fileName);
            this.latch = latch;
            this.expectSize = size;
        }

        /* (non-Javadoc)
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            initThread();
            log("Write to '" + targetFile.getAbsolutePath() + "'");
            writeToFile();
//            this.printContent();
            this.latch.countDown();
        }

        private void printContent() {
            while(this.buff.hasRemaining()) {
                log(this.buff.get());
            }
        }

        private void writeToFile() {

            FileChannel fc = null;
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(this.targetFile);
                fc = fos.getChannel();
                int s = fc.write(this.buff);
                this.isOk = (s == this.expectSize);
                log("isOK : " + this.isOk + ", " + s );
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                closeQuietly(fc);
            }
        }

        private void initThread() {
            Thread.currentThread().setName("Work-" + this.targetFile.getName());
        }

        public File getFile() {
            return this.targetFile;
        }
    }

    private void closeQuietly(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
            }
        }
    }

    private void log(Object message, Object... args) {
        log(message != null ? message.toString() : "", args);
    }

    private void log(String message, Object... args) {
        String msg = String.format(message, args);

        System.out.println(String.format("%s [%s] %s", new Date().toString(), Thread.currentThread().getName(), msg));
    }

    /**
     * @param prefix the prefix to set
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    /**
     * @param suffix the suffix to set
     */
    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }
}

