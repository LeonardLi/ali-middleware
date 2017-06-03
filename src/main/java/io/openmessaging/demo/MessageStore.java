package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class MessageStore {

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
                File file = new File(properties.getString("STORE_PATH")+"/"+bucket+":"+Thread.currentThread().hashCode());
                if(!file.exists()){
                    file.createNewFile();
                }
                bw = new BufferedWriter(new FileWriter(file));
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
        //同一个线程的所有readers
        HashMap<String,List<BufferedReader>> readers = allThreadReaders.get(queue);
        if(readers == null){
            readers = new HashMap<>();
            allThreadReaders.put(queue,readers);
        }


        //获取某一个bucket的所有bufferedreader
        ArrayList<BufferedReader> bfs = (ArrayList<BufferedReader>) readers.get(bucket);
        if(bfs == null){
            bfs = new ArrayList<>();
            ArrayList<String> filenames = bucketFilesNameMap.get(bucket);
            for(String filename : filenames){
                File file = new File(properties.getString("STORE_PATH")+"/"+filename);
                try {
                    BufferedReader bf = new BufferedReader(new FileReader(file));
                    bfs.add(bf);
                }catch (FileNotFoundException e){
                    e.printStackTrace();
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
            readers.put(bucket,bfs);
        }

        BufferedReader bf;
        String line;
        for (int i = 0 ;i < bfs.size();i++){
            bf = bfs.get(i);
            try {
                line = bf.readLine();
                //System.out.println(line);
                if (line == null || line.length() == 0) {
                    bf.close();
                    bfs.remove(bf);
                } else {
                    return stringToMessage(line);
                }
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        return null;
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
            result.append(key+":"+((DefaultKeyValue)message.headers()).get(key)+",");
        }
        result.append(";");
        //properties可能为空
        if(message.properties() !=null ){
            for(String key: message.properties().keySet()){
                result.append(key+":"+((DefaultKeyValue)message.properties()).get(key)+",");
            }
        }
        result.append(";");

        result.append(new String(message.getBody()));
        return result.toString();
    }

    private Message stringToMessage(String line){
        String[] segments = line.split(";");
        DefaultBytesMessage message = new DefaultBytesMessage("".getBytes());
        String[] headerKvs= null;
        String[] propertiesKvs = null;

        for(int i=0;i<segments.length;i++){
            if(0 == i){
                //必然有header故不再检验
                headerKvs = segments[0].split(",");
                for(String kvs : headerKvs){
                    String[] kv = kvs.split(":");
                    message.putHeaders(kv[0],kv[1]);
                }
            }
            if(1 == i) {
                if(segments[1]!=null && segments[1].length()!=0){
                    propertiesKvs = segments[1].split(",");
                    for(String kvs : propertiesKvs){
                        String[] kv = kvs.split(":");
                        message.putProperties(kv[0],kv[1]);
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
