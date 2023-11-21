package aut.db;
import java.io.*;

import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import java.lang.*;
import java.util.Arrays;
import java.util.Scanner;
import java.nio.charset.StandardCharsets;




public class Main {


public static boolean search(RocksDB rocksDB, String name) {


    RocksIterator iter = rocksDB.newIterator();
    iter.seekToFirst();
    while (iter.isValid()) {
        String key = new String(iter.key(), StandardCharsets.UTF_8);
        String val = new String(iter.value(), StandardCharsets.UTF_8);
        if (key.equals(name)) {
            //System.out.println("find");
            return true;
        }
        else {
            iter.next();
            //System.out.println("iterating");
        }
    }
    return false;
}

    public static void main(String[] args) throws IOException {


        RocksDB.loadLibrary();

        try (final Options options = new Options().setCreateIfMissing(true)) {


            try (final RocksDB db = RocksDB.open(options, "C:/Users/Kiana/Desktop/rocksdb")) {

                String csvFile = "C:/Users/Kiana/Desktop/Project0_RocksDB/American Stock Exchange 20200206_Names_ClosedVal.csv";
                String line = "";
                String cvsSplitBy = ",";

                try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

                    while ((line = br.readLine()) != null) {


                        String[] information = line.split(cvsSplitBy);

                       //System.out.println("row [name= " + information[0] + " , value=" + information[1] + "]");
                       db.put(information[0].getBytes(),information[1].getBytes());


                    }

                    Scanner scanner=new Scanner(System.in);


                    for(int i=0;;i++){

                        String instruction=scanner.next();

                        if (instruction.equals("create")){
                            String key=scanner.next();
                            String value=scanner.next();

                            if(!search(db,key)) {
                                db.put(key.getBytes(),value.getBytes());
                                System.out.println("true");
                            }
                            else
                                System.out.println("false");
                        }
                        else if(instruction.equals("fetch")){
                            String key=scanner.next();
                            if(search(db,key)) {
                                System.out.println("true");
                                String decode;
                                decode = new String(db.get(key.getBytes()),"UTF-8");
                                System.out.println(decode);
                            }
                            else
                                System.out.println("false");

                        }
                        else if (instruction.equals("delete")){
                            String key=scanner.next();
                            if(search(db,key)){
                                System.out.println("true");
                                db.delete(key.getBytes());
                            }
                            else
                                System.out.println("false");
                        }
                        else if(instruction.equals("update")){
                            String key = scanner.next();
                            String value=scanner.next();
                            if(search(db,key)){
                                System.out.println("true");
                                db.put(key.getBytes(),value.getBytes());
                            }
                            else
                                System.out.println("false");

                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                }


            }

        } catch (RocksDBException e) {

        }




}


}
