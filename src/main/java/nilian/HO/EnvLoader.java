package nilian.HO;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class EnvLoader {
    public static Properties loadEnv(String filePath) {
        Properties props = new Properties();

        try (FileInputStream fis = new FileInputStream(filePath)) {
            props.load(fis);
        } catch (IOException e) {
            e.printStackTrace(System.out);
        }

        return props;
    }
}