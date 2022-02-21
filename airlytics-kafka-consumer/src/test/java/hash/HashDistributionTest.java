package hash;

import com.ibm.airlytics.utilities.Hashing;
import scala.collection.script.NoLo$;

import java.io.*;
import java.util.UUID;

public class HashDistributionTest {
    private static final int NUMBER_OF_GUIDS = 10000000; //10M
    private static final int NUMBER_OF_SHARDS = 1000;
    public static void main(String[] args) {
        if (args.length<2) {
            System.out.println("Wrong arguments. Usage: <mode(create-file/calc-dist)> <output-file> [hash(murmure/sha256/md5)]  [input-file]");
            System.exit(-1);
        }

        String mode = args[0];
        String outputFilePath = args[1];


        try {
            switch (mode) {
                case "create-file":

                    createGuidsFile(outputFilePath);
                    break;
                case "calc-dist":
                    String hash = args[2];
                    String inputFilePath = args[3];
                    createDistributionFile(inputFilePath, outputFilePath, hash);
                    break;
                default:
                    System.out.println("Wrong mode. Can be either create-file or calc-dist)");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("DONE");
    }

    private static void createGuidsFile(String outputFilePath) throws IOException {
        File fout = new File(outputFilePath);
        FileOutputStream fos = new FileOutputStream(fout);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        for (int i = 0; i < NUMBER_OF_GUIDS; i++) {
            bw.write(UUID.randomUUID().toString());
            bw.newLine();
        }

        bw.close();
    }

    private static void createDistributionFile(String inputFilePath, String outputFilePath, String hash) throws IOException {
        FileInputStream fis = null;
        BufferedReader reader = null;
        Hashing h = new Hashing(NUMBER_OF_SHARDS);

        fis = new FileInputStream(inputFilePath);
        reader = new BufferedReader(new InputStreamReader(fis));

        String line = reader.readLine();
        Long[] shardCounters = new Long[NUMBER_OF_SHARDS];
        for (int i = 0; i < NUMBER_OF_SHARDS; i++) {
            shardCounters[i] = new Long(0);
        }

        int counter = 0;
        while (line != null && !line.isEmpty()) {
            counter++;
            UUID guid = UUID.fromString(line);
            switch (hash) {
                case "murmur":
                    int hashVal = h.hashMurmur2(guid.toString());
                    shardCounters[hashVal] = shardCounters[hashVal] + 1;
                    break;
                case "md5":
                    int hashValMD = h.hashMD5(guid.toString());
                    shardCounters[hashValMD] = shardCounters[hashValMD] + 1;
                    break;
                case "sha256":
                    int hashValSha256 = h.hashSHA256(guid.toString());
                    shardCounters[hashValSha256] = shardCounters[hashValSha256] + 1;
                    break;
                default:
                    System.out.println("Wrong hash. Can be either murmur, md5 or sha256)");
            }

            if (counter%100000 == 0) {
                System.out.println(counter);
            }

            line = reader.readLine();
        }

        File fout = new File(outputFilePath);
        FileOutputStream fos = new FileOutputStream(fout);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        for (int i = 0; i < NUMBER_OF_SHARDS; i++) {
            bw.write(shardCounters[i].toString());
            if (i<NUMBER_OF_SHARDS-1)
            bw.write(',');
        }

        bw.close();
    }
}
