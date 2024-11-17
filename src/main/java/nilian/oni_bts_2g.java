package nilian;

import nilian.Reader.FileReader;
import nilian.Sink.Batch.PostgresBatchSink;
import nilian.Sink.JdbcStatement;
import nilian.Tuple.Tuple13;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class oni_bts_2g {
    public static void main(String[] args) throws SQLException {
        //readings CSV file
        List<String> csvFile = FileReader.read(
                "/home/user/IdeaProjects/ONI-POSTGRES-TABLES/CSVs/oni_bts_2g-.csv");

        // parsing to csv
        List<Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>>
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

        PostgresBatchSink
<Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>>
                sinker = getBatchSink();

        for(Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>
        data : parsedCsv) {
            sinker.sinkData(data);
        }


    }

    private static PostgresBatchSink<Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>> getBatchSink() throws SQLException {
        JdbcStatement<Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>>
                jdbcStatement = new JdbcStatement<Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>>() {
            @Override
            public void setSen(PreparedStatement preparedStatement, Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer> data) throws SQLException {
                if(data._1 == null){
                    preparedStatement.setNull(1, Types.INTEGER);
                } else {
                    preparedStatement.setInt(1, data._1);
                }

                if(data._2 == null) {
                    preparedStatement.setNull(2, Types.VARCHAR);
                } else {
                    preparedStatement.setString(2, data._2);
                }

                if(data._3 == null) {
                    preparedStatement.setNull(3, Types.VARCHAR);
                } else {
                    preparedStatement.setString(3, data._3);
                }

                if(data._4 == null) {
                    preparedStatement.setNull(4, Types.VARCHAR);
                } else {
                    preparedStatement.setString(4, data._4);
                }

                if(data._5 == null) {
                    preparedStatement.setNull(5, Types.VARCHAR);
                } else {
                    preparedStatement.setString(5, data._5);
                }

                if(data._6 == null) {
                    preparedStatement.setNull(6, Types.DOUBLE);
                } else {
                    preparedStatement.setDouble(6, data._6);
                }

                if(data._7 == null) {
                    preparedStatement.setNull(7, Types.DOUBLE);
                } else {
                    preparedStatement.setDouble(7, data._7);
                }

                if(data._8 == null) {
                    preparedStatement.setNull(8, Types.INTEGER);
                } else {
                    preparedStatement.setInt(8, data._8);
                }

                if(data._9 == null) {
                    preparedStatement.setNull(9, Types.VARCHAR);
                } else {
                    preparedStatement.setString(9, data._9);
                }

                if(data._10 == null) {
                    preparedStatement.setNull(10, Types.VARCHAR);
                } else {
                    preparedStatement.setString(10, data._10);
                }

                if(data._11 == null) {
                    preparedStatement.setNull(11, Types.VARCHAR);
                } else {
                    preparedStatement.setString(11, data._11);
                }

                if(data._12 == null) {
                    preparedStatement.setNull(12, Types.INTEGER);
                } else {
                    preparedStatement.setInt(12, data._12);
                }

                if(data._13 == null) {
                    preparedStatement.setNull(13, Types.INTEGER);
                } else {
                    preparedStatement.setInt(13, data._13);
                }
            }
        };

        return new PostgresBatchSink<Tuple13<Integer, String, String, String, String, Double, Double, Integer, String, String, String, Integer, Integer>>(
                "jdbc:postgresql://172.30.104.47:5432/t4db",
                "1234",
                "zanboor",
            "oni_bts_2g",
                "id, region, province, city, cell_name, lat, lng, azimuth, vendor, bsc, site_name, lac, cid", jdbcStatement);
    }
}