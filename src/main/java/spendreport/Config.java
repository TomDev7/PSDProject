package spendreport;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

    private InputStream inputStream;
    private Properties configProperties;
    private static Config instance;

    private Config() throws IOException {
        try {
            Properties prop = new Properties();
            String propFileName = "config.properties";

            inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            inputStream.close();
        }
    }

    synchronized public static Config getInstance() throws Exception {
        if(instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public Double getMean(int columnNbr) {
        String key = String.valueOf(columnNbr) + "_mean";
        String mean = (String) this.configProperties.getOrDefault(key, "0");
        return Double.parseDouble(mean);
    }

    public Double getMedian(int columnNbr) {
        String key = String.valueOf(columnNbr) + "_median";
        String median = (String) this.configProperties.getOrDefault(key, "0");
        return Double.parseDouble(median);
    }

    public Double getQuantile(int columnNbr) {
        String key = String.valueOf(columnNbr) + "_quantile";
        String quantile = (String) this.configProperties.getOrDefault(key, "0");
        return Double.parseDouble(quantile);
    }
    
    public Double getMean10Lowest(int columnNbr) {
        String key = String.valueOf(columnNbr) + "_mean10Lowest";
        String mean10Lowest = (String) this.configProperties.getOrDefault(key, "0");
        return Double.parseDouble(mean10Lowest);
    }

    public Double getMb1(int columnNbr) {
        String key = String.valueOf(columnNbr) + "_mb1";
        String mb1 = (String) this.configProperties.getOrDefault(key, "0");
        return Double.parseDouble(mb1);
    }

    public Double getMb2(int columnNbr) {
        String key = String.valueOf(columnNbr) + "_mb2";
        String mb2 = (String) this.configProperties.getOrDefault(key, "0");
        return Double.parseDouble(mb2);
    }
}
