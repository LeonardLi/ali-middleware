package io.openmessaging.demo;


import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.tester.Constants;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.rmi.server.ExportException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Created by xiaode on 6/2/17.
 */
public class MyTester {

    public static void main(String[] args) throws InterruptedException{
        CountDownLatch latch = new CountDownLatch(100);
        TestConcurrentWrite[] tests = new TestConcurrentWrite[100];
        for (TestConcurrentWrite test: tests) {
            test = new TestConcurrentWrite("A",latch);
            Thread thread = new Thread(test);
            thread.start();
            thread.join();
        }
        //latch.await();

        //Thread.sleep(1000);
        TestConcurrentWrite.flush();
//        try {
//            RandomAccessFile aFile = new RandomAccessFile(Constants.STORE_PATH + "/test2", "rw");
//            FileChannel fc = aFile.getChannel();
//            String newData = "New String to write to file..." + System.currentTimeMillis();
//            ByteBuffer buf = ByteBuffer.allocate(48);
//            buf.clear();
//            buf.put(newData.getBytes());
//            buf.flip();
//            while (buf.hasRemaining()) {
//                fc.write(buf);
//            }
//        } catch(Exception e){
//            e.printStackTrace();
//        }

    }

     static String messageToString(DefaultBytesMessage message){
        StringBuffer result= new StringBuffer();

        for(String key: message.headers().keySet()){
           result.append(key+":"+((DefaultKeyValue)message.headers()).get(key)+",");
        }
        result.append(";");

        if(message.properties()!=null){
            for(String key: message.properties().keySet()){
                result.append(key+":"+((DefaultKeyValue)message.properties()).get(key)+",");
            }
        }
         result.append(";");

        result.append(new String(message.getBody()));
        return result.toString();
    }

    static Message StringToMessage(String line){
        String[] segments = line.split(";");
        DefaultBytesMessage message = new DefaultBytesMessage();
        String[] headerKvs= null;
        String[] propertiesKvs = null;

        //必然有header故不再检验
        headerKvs = segments[0].split(",");

        if(segments[1]!=null && segments[1].length()!= 0) {
            propertiesKvs = segments[1].split(",");
        }
        for(String kvs : headerKvs){
            String[] kv = kvs.split(":");
            message.putHeaders(kv[0],kv[1]);
        }
        if(propertiesKvs!= null){
           for(String kvs : propertiesKvs){
               String[] kv = kvs.split(":");
               message.putProperties(kv[0],kv[1]);
           }
        }
        if(segments[2]!= null && segments.length!=0) message.setBody(segments[2].getBytes());
       return message;
    }


}
class TestConcurrentWrite implements Runnable {
    private ThreadLocal<String> tags = new ThreadLocal<>();
    CountDownLatch latch;
    public TestConcurrentWrite(String tag,CountDownLatch latch) {
        tags.set(tag);
        this.latch = latch;
    }
    static ByteBuffer buffer = ByteBuffer.allocate(10240);


    @Override
    public void run() {
        //super.run();

            String newData = "New String to write to file..."+Thread.currentThread()+":"+System.currentTimeMillis()+"\n";
            buffer.put(newData.getBytes());
            latch.countDown();
    }

    static void flush(){
        try {
            File file = new File(Constants.STORE_PATH + "/test3");

            FileOutputStream fos = new FileOutputStream(file,true);
            FileChannel fc = fos.getChannel();
            buffer.flip();
            while(buffer.hasRemaining()){
                fc.write(buffer);
            }
            buffer.clear();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}