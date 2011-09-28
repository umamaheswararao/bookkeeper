package org.apache.bookkeeper.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class MainUtil {
    private static final String SCM_ID_PROPERTY = "scmId";
    private static final String BUILD_PROPERTIES = "/build.properties";
    static Logger LOG = Logger.getLogger(MainUtil.class);
    public static void outputInitInfo() {
        InputStream buildInfoStream = MainUtil.class.getResourceAsStream(BUILD_PROPERTIES);
        if (buildInfoStream != null) {
            Properties buildProperties = new Properties();
            try {
                buildProperties.load(buildInfoStream);
            } catch (IOException e) {
            }
            String buildVersion = buildProperties.getProperty(SCM_ID_PROPERTY);
            if (buildVersion != null) {
                LOG.info("SCM Version: " + buildVersion);
            }
        }
    }
}
