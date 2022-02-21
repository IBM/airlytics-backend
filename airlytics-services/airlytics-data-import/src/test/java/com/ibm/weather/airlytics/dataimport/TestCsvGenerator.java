package com.ibm.weather.airlytics.dataimport;

import com.opencsv.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

public class TestCsvGenerator {

    DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
            .withZone(ZoneId.systemDefault());

    @Test
    public void generateCsv() throws Exception {
        List<String> states = readStateCodes();

        File f = new File("src/test/resources/users.csv");
        String destName = "user_features_1M_" + DATE_TIME_FORMATTER.format(Instant.now());
        Path destCsv = Paths.get("test/" + destName + ".csv");
        Path destGz = Paths.get("test/" + destName + ".csv.gz");
        Random random = new Random();

        try(Reader reader = Files.newBufferedReader(f.toPath())) {

            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withQuoteChar('\"')
                    .build();

            try (CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withSkipLines(1)
                    .withCSVParser(parser)
                    .build();
                 Writer writer = Files.newBufferedWriter(destCsv);
                 CSVWriter csvWriter = new CSVWriter(writer,
                         CSVWriter.DEFAULT_SEPARATOR,
                         CSVWriter.NO_QUOTE_CHARACTER,
                         CSVWriter.NO_ESCAPE_CHARACTER,
                         CSVWriter.DEFAULT_LINE_END);) {
                String[] header = {"\"user_id\"","\"ai_core_notification_interacted_temperatures_d28\"","\"ai_core_geo_current_states_d28\""};
                csvWriter.writeNext(header);
                String[] users;
                String[] line;

                while ((users = csvReader.readNext()) != null) {
                    line = new String[3];
                    line[0] = "\"" + users[0] + "\"";
                    line[1] = String.valueOf(random.nextInt(100));
                    line[2] = "\"{" + states.get(random.nextInt(states.size())) + "," + states.get(random.nextInt(states.size())) + "}\"";
                    csvWriter.writeNext(line);
                }
            }
        }
        compressGzip(destCsv, destGz);
    }

    private List<String> readStateCodes() throws Exception {
        List<String> result = new LinkedList<>();
        File f = new File("src/test/resources/states.csv");

        try(Reader reader = Files.newBufferedReader(f.toPath())) {

            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withQuoteChar('\"')
                    .build();

            try (CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withSkipLines(1)
                    .withCSVParser(parser)
                    .build()) {
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    result.add(line[1]);
                }
            }
        }
        return result;
    }

    private void compressGzip(Path source, Path target) throws IOException {

        try (GZIPOutputStream gos = new GZIPOutputStream(
                new FileOutputStream(target.toFile()));
             FileInputStream fis = new FileInputStream(source.toFile())) {

            // copy file
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gos.write(buffer, 0, len);
            }

        }
    }
}
