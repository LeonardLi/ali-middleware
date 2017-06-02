package io.openmessaging.demo;


import java.io.*;

/**
 * Created by xiaode on 6/2/17.
 */
public class MyTester {

    public static void main(String[] args){
        DefaultBytesMessage message = new DefaultBytesMessage("Test".getBytes());
        message.putHeaders("amount",10);
        message.putProperties("data","ted");
        message.setBody("hhhhhhhh".getBytes());
        String result = messageToString(message);
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

    public static String messageToString(DefaultBytesMessage message){
        String result= "";
        for(String key: message.headers().keySet()){
            result+=(key+":"+message.headers().getString(key)+",");
        }
        result+=";";
        for(String key: message.properties().keySet()){
            result+=(key+":"+message.properties().getString(key)+",");
        }
        result+=";";
        result+= new String(message.getBody());
        return result;
    }
}
