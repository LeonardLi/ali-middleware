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

    private HashMap<String,BufferedWriter> writerBuckets = new HashMap<>();

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
        BufferedWriter bw =  writerBuckets.get(name);
        try{
            if (bw == null){
                File file = new File(properties.getString("STORE_PATH")+"/"+name);
                if(!file.exists()){
                    file.createNewFile();
                }
                bw = new BufferedWriter(new FileWriter(file));
                writerBuckets.put(name,bw);
            }
        } catch(IOException e){
            e.printStackTrace();
        }

        try {
            bw.write(messageToString(message1)+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private String messageToString(DefaultBytesMessage message){
        StringBuffer result= new StringBuffer();

        for(String key: message.headers().keySet()){
            result.append(key+":"+message.headers().get(key)+",");
        }
        result.append(";");

        if(message.properties()!=null){
            for(String key: message.properties().keySet()){
                result.append(key+":"+message.properties().get(key)+",");
            }
        }
        result.append(";");

        result.append(new String(message.getBody()));
        return result.toString();
    }

    private Message stringToMessage(String line){
        String[] segments = line.split(";");
        DefaultBytesMessage message =  new DefaultBytesMessage(segments[2].getBytes());


        String[] headerKvs= null;
        String[] propertiesKvs = null;
        if(!segments[0].equals("")){
            headerKvs = segments[0].split(",");
        }
        if(!segments[1].equals("")) {
            propertiesKvs = segments[1].split(",");
        }
        if(headerKvs!=null){
            for(String kvs : headerKvs){
                String[] kv = kvs.split(":");
                message.putHeaders(kv[0],kv[1]);
            }
        }

        if(propertiesKvs!= null){
            for(String kvs : propertiesKvs){
                String[] kv = kvs.split(":");
                message.putProperties(kv[0],kv[1]);
            }
        }
        return message;
    }

    private HashMap <String,HashMap<String,BufferedReader>> allThreadReaders = new HashMap<>();
   public  Message pullMessage(String queue, String bucket) {
       HashMap<String,BufferedReader> readers = allThreadReaders.get(queue);
       if(readers == null){
           readers = new HashMap<>();
           allThreadReaders.put(queue,readers);
       }
       BufferedReader bf = readers.get(queue);
       String line = null;
       try{
           if(bf == null){
               File file = new File(properties.getString("STORE_PATH")+"/"+queue);
               bf = new BufferedReader(new FileReader(file));
               readers.put(queue,bf);
           }
       } catch (FileNotFoundException e){
           e.printStackTrace();
       }
        try {
            line = bf.readLine();
        } catch (IOException e){
           e.printStackTrace();
        }

        if(line != null && !line.equals("")){
            return stringToMessage(line); //String to message
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
