/******************************************************************************/
/* SYSTEM     : IM Step2                                                        */
/*                                                                            */
/* SUBSYSTEM  :                                                            */
/******************************************************************************/
package org.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 *
 * @author  Lin zhanwang
 * @version 1.0
 * @since   1.0
 *
 * <MODIFICATION HISTORY>
 *  (Rev.)		(Date)     	(Name)        (Comment)
 *  1.0    		2016年10月20日    	Lin zhanwang       New making
 */
public class TestFileSplit {

    private static final String FILE_PATH = "F:\\tmp\test.txt";
    private static final String EAR_PATH = "F:\\tmp\\pp\\a1stream.ear";
    private static final String OUT_FILE_PATH = "F:\\tmp\\out.txt";
    private static final String TEST_FILE = "F:\\tmp\\test_out.txt";
    private static final int BSIZE = 1024;

    private static int numOfInts = 4000000;
    private static int numOfUbuffInts = 200000;

    public static void main(String[] args) throws Exception {

        long limitSize = 1024 * 1024; // 1M
//         FileChannel fc = new RandomAccessFile(EAR_PATH, "r").getChannel();
        FileSplitter fsp = new FileSplitter(new File(EAR_PATH), 10, "f:/tmp/pp");
        fsp.setPrefix("a1stream-");
        fsp.setSuffix(".part");
        File[] fs = fsp.start(); //Split to 10 files.

        log(fs.length);

         log("-----------");
         fs = new FileSplitter(new File(EAR_PATH), 1024 * 1024 * 10L, "f:/tmp/pp/2").start(); // Split to the 10M-per-files.
         log(fs.length);

//        testBig();
    }

    public static void testBig() {

        String bigExcelFile = EAR_PATH;
        // Create file object
        File file = new File(bigExcelFile);

        // Get file channel in readonly mode
        FileChannel fileChannel;
        try {
            fileChannel = new RandomAccessFile(file, "r").getChannel();


            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());

            // the buffer now reads the file as if it were loaded in memory.
            System.out.println(buffer.isLoaded()); // prints false
            System.out.println(buffer.capacity() +"," +buffer.limit()); // Get the size based on content size of file

            System.out.println(buffer.hasRemaining());
            // You can read the file from this buffer the way you like.
//            for (int i = 0; i < buffer.limit(); i++) {
//                System.out.print((char) buffer.get()); // Print the content of file
//            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }



    public static void closeQuietly(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
            }
        }
    }

    public static void log(Object message, Object... args) {
        log(message != null ? message.toString() : "", args);
    }

    public static void log(String message, Object... args) {
        String msg = String.format(message, args);

        System.out.println(String.format("%s [%s] %s", new Date().toString(), Thread.currentThread().getName(), msg));
    }

    public static void test6() {
    }

    private abstract static class Tester {
        private String name;

        public Tester(String name) {
            this.name = name;
        }

        // 效率测试模板方法
        public void runTest() {
            log(name + " : ");
            try {
                long start = System.nanoTime();
                test();
                double duration = System.nanoTime() - start;
                System.out.format("%.2f\n", duration / 1.0e9);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public abstract void test() throws IOException;
    }

    public static void test5() {

        Tester[] testers = { new Tester("Stream Write") {

            @Override
            public void test() throws IOException {
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(TEST_FILE)));
                int i = 0;
                while (i++ < numOfInts) {
                    out.writeInt(i);
                }
                out.close();
            }
        }, new Tester("Mapped Write") {

            @Override
            public void test() throws IOException {
                FileChannel fc = new RandomAccessFile(TEST_FILE, "rw").getChannel();
                IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size()).asIntBuffer();
                int i = 0;
                while (i++ < numOfInts) {
                    ib.put(i);
                }
                fc.close();
            }
        }, new Tester("Stream Read") {

            @Override
            public void test() throws IOException {
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(TEST_FILE)));
                int i = 0;
                while (i++ < numOfInts) {
                    in.readInt();
                }
                in.close();
            }
        }, new Tester("Mapped Read") {

            @Override
            public void test() throws IOException {
                FileChannel fc = new FileInputStream(TEST_FILE).getChannel();
                IntBuffer ib = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).asIntBuffer();
                ib.rewind();
                while (ib.hasRemaining()) {
                    ib.get();
                }
                fc.close();
            }
        }, new Tester("Stream Read/Write") {

            @Override
            public void test() throws IOException {
                RandomAccessFile raf = new RandomAccessFile(TEST_FILE, "rw");
                raf.writeInt(1);
                int i = 0;
                while (i++ < numOfUbuffInts) {
                    raf.seek(raf.length() - 4);
                    raf.writeInt(raf.readInt());
                }
                raf.close();
            }
        }, new Tester("Mapped Read/Write") {

            @Override
            public void test() throws IOException {
                FileChannel fc = new RandomAccessFile(TEST_FILE, "rw").getChannel();
                IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size()).asIntBuffer();
                ib.put(0);
                int i = 0;
                while (i++ < numOfUbuffInts) {
                    ib.put(ib.get(i - 1));
                }
                fc.close();
            }
        } };

        for (Tester tester : testers) {
            tester.runTest();
        }
    }

    public static void test4() {
        ByteBuffer bb = ByteBuffer.wrap(new byte[] { 0, 0, 0, 0, 0, 0, 0, 'a' });

        // ByteBuffer
        bb.rewind();
        System.out.print("ByteBuffer: ");
        while (bb.hasRemaining()) {
            System.out.print(bb.position() + " -> " + bb.get() + ", ");
        }

        // CharBuffer
        bb.rewind();
        CharBuffer cb = bb.asCharBuffer();
        System.out.print("CharBuffer: ");
        while (cb.hasRemaining()) {
            System.out.print(cb.position() + " -> " + cb.get() + ", ");
        }

        // ShortBuffer
        bb.rewind();
        ShortBuffer sb = bb.asShortBuffer();
        System.out.print("ShortBuffer: ");
        while (sb.hasRemaining()) {
            System.out.print(sb.position() + " -> " + sb.get() + ", ");
        }

        // IntBuffer
        bb.rewind();
        IntBuffer ib = bb.asIntBuffer();
        System.out.print("IntBuffer: ");
        while (ib.hasRemaining()) {
            System.out.print(ib.position() + " -> " + ib.get() + ", ");
        }

        // LongBuffer
        bb.rewind();
        LongBuffer lb = bb.asLongBuffer();
        System.out.print("LongBuffer: ");
        while (lb.hasRemaining()) {
            System.out.print(lb.position() + " -> " + lb.get() + ", ");
        }

        // FloatBuffer
        bb.rewind();
        FloatBuffer fb = bb.asFloatBuffer();
        System.out.print("FloatBuffer: ");
        while (fb.hasRemaining()) {
            System.out.print(fb.position() + " -> " + fb.get() + ", ");
        }

        // DoubleBuffer
        bb.rewind();
        DoubleBuffer db = bb.asDoubleBuffer();
        System.out.print("DoubleBuffer: ");
        while (db.hasRemaining()) {
            log(db.position() + " -> " + db.get() + ", ");
        }
    }

    public static void test3() throws Exception {
        FileChannel fc = new FileOutputStream(OUT_FILE_PATH).getChannel();
        fc.write(ByteBuffer.wrap("test data".getBytes()));
        fc.close();

        ByteBuffer bb = ByteBuffer.allocate(BSIZE);
        fc = new FileInputStream(OUT_FILE_PATH).getChannel();
        fc.read(bb);
        fc.close();

        // 编码有问题情况:写入的时候使用的是UTF-8,而ByteBuffer.asCharBuffer()的解码编码是UTF16-BE
        bb.flip();
        log("Bad encoding : " + bb.asCharBuffer());

        // 使用指定编码UTF-8 decode
        String encoding = System.getProperty("file.encoding");
        log("Using charset '" + encoding + "' : " + Charset.forName(encoding).decode(bb));

        // 或者使用指定编码UTF-16写入文件
        fc = new FileOutputStream(OUT_FILE_PATH).getChannel();
        fc.write(ByteBuffer.wrap("test data".getBytes("UTF-16BE")));
        fc.close();

        bb.clear();
        fc = new FileInputStream(OUT_FILE_PATH).getChannel();
        fc.read(bb);
        fc.close();

        bb.flip();
        log("Using charset 'UTF-16BE' to write file : " + bb.asCharBuffer());

        // 或者选择直接使用CharBuffer写入
        bb.clear();
        bb.asCharBuffer().put("test data 2");
        fc = new FileOutputStream(OUT_FILE_PATH).getChannel();
        fc.write(bb);
        fc.close();

        fc = new FileInputStream(OUT_FILE_PATH).getChannel();
        bb.clear();
        fc.read(bb);
        bb.flip();
        log("Use CharBuffer to write file : " + bb.asCharBuffer());
    }

    private void test2() {

        try {
            FileChannel fc = new FileOutputStream(FILE_PATH).getChannel();
            fc.write(ByteBuffer.wrap("Some data ".getBytes()));
            fc.close();

            // RandomAccessFile
            fc = new RandomAccessFile(FILE_PATH, "rw").getChannel();
            fc.position(fc.size()); // 移动到文件尾
            fc.write(ByteBuffer.wrap("some more data".getBytes()));
            fc.close();

            // InputStream
            ByteBuffer bb = ByteBuffer.allocate(BSIZE);
            log(bb.toString());
            fc = new FileInputStream(FILE_PATH).getChannel();
            fc.read(bb);
            bb.flip(); // 将limit设为position并将position设为0
            while (bb.hasRemaining()) {
                System.out.print((char) bb.get());
            }
            fc.close();

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     *
     */
    private static void test1() {
        FileInputStream fis = null;
        FileChannel fc = null;
        try {

            String fileName = "F:\\tmp\\test.xls";
            File matrixFile = new File(fileName);
            if (!matrixFile.exists()) {
                throw new FileNotFoundException(fileName);
            }

            long fsize = matrixFile.length();

            //
            fis = new FileInputStream(matrixFile);
            fc = fis.getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
            CharBuffer charBuffer = CharBuffer.allocate(1024);
            Charset charset = Charset.forName("UTF-8");
            CharsetDecoder decoder = charset.newDecoder();
            while (fc.read(buffer) != -1) {
                buffer.flip();// 写出前操作
                // while (buffer.hasRemaining()) {//枚举byteBuffer中的数据
                decoder.decode(buffer, charBuffer, false);
                charBuffer.flip();
                // }
                buffer.clear();// 读入前操作
                charBuffer.clear();
            }
        } catch (Exception ffe) {
            ffe.printStackTrace();
        } finally {
            try {
                if (fc != null)
                    fc.close();
                if (fis != null)
                    fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
