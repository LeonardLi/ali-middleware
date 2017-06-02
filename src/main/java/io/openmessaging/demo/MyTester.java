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

        try{
            File file = new File("test.dat");
            if(!file.exists()){
                file.createNewFile();
            }
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
            oos.writeObject(message);
            oos.writeObject(message);

            System.out.println("finish write");

            oos.close();
            FileInputStream fn = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fn);
            while (fn.available()> 0){

                DefaultBytesMessage message1 = (DefaultBytesMessage) ois.readObject();
                System.out.println(message1.getBody());
                System.out.println(message1.headers().getInt("amount"));
                System.out.println(message1.properties().getString("data"));
            }


            ois.close();

        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }catch (ClassNotFoundException e){
            e.printStackTrace();
        }

    }
}
