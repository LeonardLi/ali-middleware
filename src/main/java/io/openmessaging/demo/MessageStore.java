package io.openmessaging.demo;

import com.sun.org.apache.xerces.internal.impl.dv.xs.BooleanDV;
import com.sun.org.apache.xpath.internal.operations.Bool;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class MessageStore {
    private Logger logger = LoggerFactory.getLogger(MessageStore.class);
    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    //每个topic或者queue的实际的文件路径
    private volatile HashMap<String,List<String>> bucketFiles;

    private volatile KeyValue properties;

    //各线程共享的bucket与文件字典
    private HashMap<String,ArrayList<String>> bucketFilesNameMap = null;
    //private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    //private ThreadLocal<HashMap<String,ObjectInputStream>> readerBuckets = new ThreadLocal<>();

    //private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    //写文件锁，可以考虑去掉
    //private ReentrantLock lock = new ReentrantLock(true);

    //线程独享的用于写文件的writers
    private ThreadLocal<HashMap<String,BufferedWriter>> writerBuckets = new ThreadLocal<>();

    //线程独享用于读文件的readers
    private HashMap <String,HashMap<String,List<BufferedReader>>> allThreadReaders = new HashMap<>();

    private ThreadLocal<char[] > bufferT = new ThreadLocal<>();

//    private ThreadLocal<String> nameT = new ThreadLocal<>();

    private ThreadLocal<ArrayList<String>> filenamesT = new ThreadLocal<>();

    private ThreadLocal<Integer> indexT = new ThreadLocal<>();

    private ThreadLocal<Integer> fileLengthT = new ThreadLocal<>();

    private ThreadLocal<Boolean> isEndT = new ThreadLocal<>();

    private ThreadLocal<String> bucketT = new ThreadLocal<>();

    /**
     * 落盘函数
     * @param bucket
     * @param message
     */
    public void putMessage(String bucket, Message message) {
//        if (!messageBuckets.containsKey(bucket)) {
//            messageBuckets.put(bucket, new ArrayList<>(1024));
//        }
        // in memory
        // ArrayList<Message> bucketList = messageBuckets.get(bucket);
        // bucketList.add(message);
        DefaultBytesMessage message1 = (DefaultBytesMessage) message;
        HashMap<String,BufferedWriter> writers = writerBuckets.get();
        if(writers == null ){
            writers = new HashMap<>();
            writerBuckets.set(writers);
        }

        BufferedWriter bw =  writers.get(bucket);
        try{
            if (bw == null){

                File file = new File(properties.getString("STORE_PATH")+"/"+bucket+":"+ Thread.currentThread().hashCode());

                if(!file.exists()){
                    file.createNewFile();
                }
                bw = new BufferedWriter(new FileWriter(file));//4M buffersize

                writers.put(bucket,bw);
            }
        } catch(IOException e){
            e.printStackTrace();
        }
        try {
            String string = messageToString(message1);
            //System.out.println(string);
            bw.write(string+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }


    /**
     * 读盘函数，这里对应落盘的设计是，每个线程持有自己的BufferedReader的Map，自己的Map中key为bucket，value是该bucket的所有文件
     * 的bufferedreader,以这种方式来实现
     *
     * @param queue
     * @param bucket
     * @return
     */
    public  Message pullMessage(String queue, String bucket) {
        ArrayList<String> filenames = filenamesT.get();
        if (filenames == null) {
            filenames = new ArrayList<>();
            for (String name : bucketFilesNameMap.get(bucket)) {
                filenames.add(name);
            }
            filenamesT.set(filenames);
        }
//        if (filenames.size() == 0) {
//            return null;
//        }
        char[] buffer = bufferT.get();
        if (buffer == null) {
            buffer = new char[4*1024*1024];
            bufferT.set(buffer);
        }

        if (isEndT.get() == null) {
            isEndT.set(true);
        }

        if (bucketT.get() == null || !bucketT.get().equals(bucket)) {
            bucketT.set(bucket);
            filenames = bucketFilesNameMap.get(bucket);
            filenamesT.set(filenames);
        }


        try {
            while (true) {
                if (isEndT.get()) {
                    if (filenames.size() == 0) {
                        return null;
                    }
                    String filename = filenames.get(0);
                    File file = new File(properties.getString("STORE_PATH") + "/" + filename);
                    BufferedReader bf = new BufferedReader(new FileReader(file), 4 * 1024 * 1024);
                    //设置文件长度
                    fileLengthT.set(bf.read(buffer));
//                    nameT.set(filename);
                    // 移除文件
                    filenames.remove(0);
                    indexT.set(0);
                    bf.close();
                    isEndT.set(false);
                }
                int len = fileLengthT.get();
                Integer index = indexT.get();
                // 已经读完了
                if (index == len) {
                    isEndT.set(true);
                    continue;
                }
                // 读文件
                StringBuilder sb = new StringBuilder();
                for (int i = index; i < len; i++) {
                    if (buffer[i] != '\n') {
                        sb.append(buffer[i]);
                    }
                    // 到了行末
                    else {
                        index = i + 1;
                        indexT.set(index);
                        break;
                    }
                }

                String line = sb.toString();
//                System.out.println(line);
//                System.out.println(line);
                return stringToMessage(line);
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;


//        //获取某一个bucket的所有bufferedreader
//        ArrayList<BufferedReader> bfs = (ArrayList<BufferedReader>) readers.get(bucket);
//        if(bfs == null){
//            bfs = new ArrayList<>();
//            ArrayList<String> filenames = bucketFilesNameMap.get(bucket);
//            for(String filename : filenames){
//                File file = new File(properties.getString("STORE_PATH")+"/"+filename);
//                try {
//                    BufferedReader bf = new BufferedReader(new FileReader(file), 4096*128);
//                    bfs.add(bf);
//                }catch (FileNotFoundException e){
//                    e.printStackTrace();
//                } catch (IOException e){
//                    e.printStackTrace();
//                }
//            }
//            readers.put(bucket,bfs);
//        }
//
//        BufferedReader bf;
//        String line;
//        for (int i = 0 ;i < bfs.size();i++){
//            bf = bfs.get(i);
//
//            try {
//                line = bf.readLine();
//
//                //System.out.println(line);
//                if (line == null || line.length() == 0) {
//                    continue;
//                    //bf.close();
//                    //bfs.remove(bf);
//                } else {
//                    //logger.error(line);
//                    return stringToMessage(line);
//                }
//            }catch(IOException e){
//                e.printStackTrace();
//            }
//        }
//        return null;
//        ArrayList<Message> bucketList = messageBuckets.get(bucket);
//        if (bucketList == null) {
//            return null;
//        }
//        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
//        if (offsetMap == null) {
//            offsetMap = new HashMap<>();
//            queueOffsets.put(queue, offsetMap);
//        }
//        int offset = offsetMap.getOrDefault(bucket, 0);
//        if (offset >= bucketList.size()) {
//            return null;
//        }
//        Message message = bucketList.get(offset);
//        offsetMap.put(bucket, ++offset);
//        return message;


    }

    public void flush(){
       HashMap<String,BufferedWriter> writers = writerBuckets.get();
        for(String name : writers.keySet()){
            try{
            writers.get(name).flush();
            writers.get(name).close();
            } catch(IOException e){
                e.printStackTrace();
            }
        }
    }
    ///////////////////////////消息互转的工具函数
    private String messageToString(DefaultBytesMessage message){
        StringBuilder result= new StringBuilder(300);
        //header不可能为空，必有topic或者queue
        for(String key: message.headers().keySet()){

            //result.append(key.charAt(0)+":"+((DefaultKeyValue)message.headers()).get(key)+",");
            String value = ((DefaultKeyValue)message.headers()).get(key);
            if('T' == key.charAt(0)|| 'Q'== key.charAt(0)){
                value = value.substring(6);
            }

            result.append(key.charAt(0))
                    .append(':')
                    .append(value)
                    .append(',');

        }
        result.append(';');
        //properties可能为空
        if(message.properties() !=null ){
            for(String key: message.properties().keySet()){

                String value = ((DefaultKeyValue)message.properties()).get(key);
                if('P' ==  key.charAt(0)){
                    value = value.substring(8);
                    key = "P";
                }
                result.append(key)
                        .append(':')
                        .append(value)
                        .append(',');
            }
        }
        result.append(";");

        result.append(new String(message.getBody()));
        return result.toString();
    }

    private Message stringToMessage(String line){
        String[] segments = line.split(";");
        DefaultBytesMessage message = new DefaultBytesMessage("".getBytes());
        String[] headerKvs;
        String[] propertiesKvs;

        for(int i=0;i < segments.length;i++){
            if(0 == i){
                //必然有header故不再检验
                headerKvs = segments[0].split(",");
                for(String kvs : headerKvs){
                    String[] kv = kvs.split(":");
                    if('T' == kv[0].charAt(0)) message.putHeaders("Topic","TOPIC_"+kv[1]);
                    else if('M' == kv[0].charAt(0)) message.putHeaders("MessageId",kv[1]);
                    else if('Q' == kv[0].charAt(0)) message.putHeaders("Queue","QUEUE_"+kv[1]);
                    else throw new RuntimeException("undefined key");
                }
            }
            if(1 == i) {
                if(segments[1]!=null && segments[1].length()!=0){
                    propertiesKvs = segments[1].split(",");
                    for(String kvs : propertiesKvs){
                        String[] kv = kvs.split(":");
                        if('P' == kv[0].charAt(0)) message.putProperties("PRO_OFFSET","PRODUCER"+kv[1]);
                        else message.putProperties(kv[0],kv[1]);
                    }
                }
            }
            if(2 == i) {
                message.setBody(segments[2].getBytes());
            }
        }

        return message;
    }

    public synchronized void initFileArray(){
        if(bucketFilesNameMap != null) return;
         bucketFilesNameMap = new HashMap<>();
        File[] fileArray;
        File file = new File(DefaultPullConsumer.properties.getString("STORE_PATH"));
        fileArray = file.listFiles();

        //将bucket和对应的文件名存储起来
        for (int i= 0;i< fileArray.length;i++){
            if(fileArray[i].isFile()){
                String name = fileArray[i].getName();
                //logger.info(name+" Size :"+fileArray[i].length());
                String []segs =name.split(":");
                if(bucketFilesNameMap.containsKey(segs[0])){
                    bucketFilesNameMap.get(segs[0]).add(name);
                }else {
                    ArrayList<String> filenames = new ArrayList<>();
                    filenames.add(name);
                    bucketFilesNameMap.put(segs[0], filenames);
                }
            }
        }
        return;
    }

    public void setProperties(KeyValue properties) {
        if(this.properties == null){
            this.properties = properties;
        }
    }
}
