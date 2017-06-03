package io.openmessaging.demo;


import io.openmessaging.Message;

import java.io.*;

/**
 * Created by xiaode on 6/2/17.
 */
public class MyTester {

    public static void main(String[] args){
        DefaultBytesMessage message = new DefaultBytesMessage("Test".getBytes());
//        message.putHeaders("amount",10);
//        message.putProperties("data","ted");
//        message.setBody("hhhhhhhh".getBytes());
        message.putHeaders("Queue","QUEUE_0");
        message.setBody("PRODUCER_7_1".getBytes());
        //Queue:QUEUE_0,;;PRODUCER_7_1
        String result = messageToString(message);
        Message message1 = StringToMessage(result);
        String[] results = result.split(";");

        try{
            File file = new File("test.dat");
            if(!file.exists()){
                file.createNewFile();
            }

            BufferedWriter bw = new BufferedWriter(new FileWriter(file));

            //ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
            //oos.writeObject(message);
            //oos.writeObject(message);
            bw.write(result+"\n");
            bw.write(result+"\n");

            System.out.println("finish write");

            bw.close();

            BufferedReader br = new BufferedReader(new FileReader(file));

            String line = br.readLine();
            System.out.println(line);


            br.close();
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

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
