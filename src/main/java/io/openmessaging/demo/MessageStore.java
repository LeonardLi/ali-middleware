package io.openmessaging.demo;

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

    private ThreadLocal<HashMap<String,ObjectOutputStream>> writerBuckets = new ThreadLocal<>();

    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    public synchronized void putMessage(String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
    }

    private void writeTofile(String name, Message message){
        HashMap<String,ObjectOutputStream> writers = writerBuckets.get();

        if(writers == null){
            writers = new HashMap<>();
            writerBuckets.set(writers);
        }

        try {
            ObjectOutputStream writer = writers.get(name);
            if(writer == null){
                File file = new File( DefaultProducer.properties.getString("STORE_PATH")+name);
                if (file.exists()) file.createNewFile();
                FileOutputStream fos = new FileOutputStream(file);
                ObjectOutputStream oos = new ObjectOutputStream(fos);
                writer = oos;
                writers.put(name,oos);
            }
            //writer.write(message);
        } catch (IOException e){
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
}
