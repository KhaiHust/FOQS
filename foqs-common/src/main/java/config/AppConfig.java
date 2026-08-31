package config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private static final Properties properties = new Properties();
    private static final String ACTIVE_PROFILE;

    static {
        loadPropertiesFile("application.properties");

        String sysProp = System.getProperty("foqs.profile");
        String envVar = System.getenv("FOQS_PROFILE");
        String propFileProfile = properties.getProperty("foqs.profile");
        ACTIVE_PROFILE = (sysProp != null && !sysProp.isBlank()) ? sysProp
                : (envVar != null && !envVar.isBlank()) ? envVar
                : (propFileProfile != null && !propFileProfile.isBlank()) ? propFileProfile
                : "local";

        logger.info("Initializing FOQS with profile: [{}]", ACTIVE_PROFILE);

        loadPropertiesFile("application-" + ACTIVE_PROFILE + ".properties");
    }

    private static void loadPropertiesFile(String fileName) {
        try (InputStream inputStream = AppConfig.class.getClassLoader().getResourceAsStream(fileName)) {
            if (inputStream != null) {
                properties.load(inputStream);
                logger.info("Loaded properties from file: {}", fileName);
            } else {
                logger.warn("Properties file not found: {}", fileName);
            }
        } catch (Exception e) {
            logger.error("Error loading properties file: {}", fileName, e);
        }
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static int getIntProperty(String propertyName, int defaultValue) {
        String value = properties.getProperty(propertyName);
        if (value != null) {
            return Integer.parseInt(value);
        }
        return defaultValue;
    }

    public static List<ShardConfig> getShardConfigs() {
        int count = getIntProperty("foqs.shards.count", 1);
        List<ShardConfig> shardConfigs = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            String prefix = "foqs.shards." + i + ".";
            String url = getProperty(prefix + "url", null);
            String user = getProperty(prefix + "username", "root");
            String pass = getProperty(prefix + "password", "root");

            if (url == null) {
                throw new IllegalStateException("Missing JDBC URL config for shard index: " + i);
            }

            shardConfigs.add(ShardConfig.builder()
                    .shardId(i)
                    .jdbcUrl(url)
                    .username(user)
                    .password(pass)
                    .build());
        }
        return shardConfigs;
    }
}
