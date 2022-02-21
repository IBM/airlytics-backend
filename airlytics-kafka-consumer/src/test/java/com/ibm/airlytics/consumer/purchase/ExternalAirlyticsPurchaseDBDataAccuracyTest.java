package com.ibm.airlytics.consumer.purchase;

import org.apache.commons.io.IOUtils;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@RunWith(Parameterized.class)
public class ExternalAirlyticsPurchaseDBDataAccuracyTest extends InternalAirlyticsPurchaseDBDataAccuracyTest{

    protected void readConfig() throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(CONFIG_FOLDER + File.separator + "ExternalAirlyticsPurchaseDBDataAccuracyTest.properties")) {
            String props = IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);

            properties = new Properties();
            InputStream is = new ByteArrayInputStream(props.getBytes());
            properties.load(is);
        }
    }
}