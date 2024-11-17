package nilian;

import nilian.Reader.FileReader;
import nilian.Sink.Batch.PostgresBatchSink;
import nilian.Tuple.Tuple13;
import nilian.Tuple.Tuple15;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class oni_bts_3g {
    public static void main(String[] args) throws SQLException {



        //readings CSV file
        List<String> csvFile = FileReader.read(
                "/home/user/IdeaProjects/ONI-POSTGRES-TABLES/CSVs/oni_bts_3g-.csv");

        // parsing to csv
        List<Tuple15<Integer, String, String, String, String, Double, Double, Integer, String, String, String, String, Integer, Integer, Integer>>
                parsedCsv = new ArrayList<>();
        for(String rawData: csvFile) {
            String[] arr = rawData.split(",");
            if(arr.length == 13) {
                try {
                    Integer int_1 = null;
                    if(!arr[0].isEmpty()) {
                        int_1 = Integer.parseInt(arr[0]);
                    }

                    Double double_2 = null;
                    if(!arr[5].isEmpty()){
                        double_2 = Double.parseDouble(arr[5]);
                    }

                    Double double_3 = null;
                    if(!arr[6].isEmpty()){
                        double_3 = Double.parseDouble(arr[6]);
                    }

                    Integer int_4 = null;
                    if(!arr[7].isEmpty()){
                        int_4 = Integer.parseInt(arr[7]);
                    }

                    Integer int_5 = null;
                    if(!arr[11].isEmpty()){
                        int_5 = Integer.parseInt(arr[11]);
                    }


                    Integer int_6 = null;
                    if(!arr[12].isEmpty()){
                        int_6 = Integer.parseInt(arr[12]);
                    }


                    parsedCsv.add(new Tuple13<>(
                            int_1,
                            arr[1],
                            arr[2],
                            arr[3],
                            arr[4],
                            double_2,
                            double_3,
                            int_4,
                            arr[8],
                            arr[9],
                            arr[10],
                            int_5,
                            int_6
                    ));
                } catch (NumberFormatException e) {
                    System.out.println("ERROR:\n"+rawData);
                    throw new RuntimeException(e);
                }
            } else {
                System.out.println("bad comma between:\n"+rawData);
            }
        }

        PostgresBatchSink<Tuple15<Integer, String, String, String, String, Double, Double, Integer, String, String, String, String, Integer, Integer, Integer>>
                sinker = new PostgresBatchSink
                <Tuple15<Integer, String, String, String, String, Double, Double, Integer, String, String, String, String, Integer, Integer, Integer>>(
                "jdbc:postgresql://172.30.104.47:5432/t4db",
                "1234",
                "zanboor",
                "oni_bts_3g",
                "id, region, province, city, cell_name, lat, lng, azimuth, vendor, bsc, site_name, lac, cid");

        for(Tuple15<Integer, String, String, String, String, Double, Double, Integer, String, String, String, String, Integer, Integer, Integer>
                data : parsedCsv) {
            sinker.sinkData(data);
        }


    }
}
