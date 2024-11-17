package nilian.Reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class FileReader {
    public static List<String> read(String path) {

        List<String> data = new ArrayList<>();
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            int i = 0 ;
            while (myReader.hasNextLine()) {
                i++;
                if(i == 1 ){
                    myReader.nextLine();
                } else {
                    data.add(myReader.nextLine());
                }
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace(System.out);
        }
        return data;
    }
}