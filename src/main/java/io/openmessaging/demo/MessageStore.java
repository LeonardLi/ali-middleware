package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    private ThreadLocal<HashMap<String,ObjectInputStream>> readerBuckets = new ThreadLocal<>();

    private HashMap<String,ObjectOutputStream> writerBuckets = new HashMap<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private KeyValue properties;

    public void setProperties(KeyValue properties) {
        this.properties = properties;
    }

    public synchronized void putMessage(String bucket, Message message) {
//        if (!messageBuckets.containsKey(bucket)) {
//            messageBuckets.put(bucket, new ArrayList<>(1024));
//        }
        // in memory
        // ArrayList<Message> bucketList = messageBuckets.get(bucket);
        // bucketList.add(message);

        this.writeTofile(bucket,message);

    }

    private void writeTofile(String name, Message message){
        DefaultBytesMessage message1 = (DefaultBytesMessage) message;
        ObjectOutputStream outputStream =  writerBuckets.get(name);
        try{
            if (outputStream == null){
                File file = new File(DefaultProducer.properties.getString("STORE_PATH")+"/"+name);
                if(!file.exists()){
                    file.createNewFile();
                }
                outputStream = new ObjectOutputStream(new FileOutputStream(file));
                writerBuckets.put(name,outputStream);
            }
        } catch(IOException e){
            e.printStackTrace();
        }

        try {
            outputStream.writeObject(message1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

   public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
   }

   public void flush(){
        for(String name : writerBuckets.keySet()){
            try{
            writerBuckets.get(name).flush();
            writerBuckets.get(name).close();
            } catch(IOException e){
                e.printStackTrace();
            }
        }
   }
}
