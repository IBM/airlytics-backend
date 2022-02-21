package hash;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.airlytics.utilities.Hashing;
import com.sangupta.murmur.Murmur2;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MurmurHashTest {
    public static void main (String[] args) {
        if (args.length<1) {
            System.out.println("missing parameter <file-path>");
            return;
        }
        String filePath = args[0];//"/Users/iritma/Downloads/murmur2_1000000-ids.txt";
        FileInputStream fis = null;
        BufferedReader reader = null;
        Hashing h = new Hashing(1000);
        ObjectMapper mapper = new ObjectMapper();
        try {
            fis = new FileInputStream(filePath);
            reader = new BufferedReader(new InputStreamReader(fis));

            System.out.println("Reading File line by line using BufferedReader");

            String line = reader.readLine();
            while (line != null) {
                JsonNode obj = mapper.readTree(line);
                String userId = obj.get("uuidv4").textValue();
                long inputHash = obj.get("hash").longValue();
                //long generatedHash = h.hashMurmur2(userId);
                byte[] value = userId.getBytes();
                long generatedHash = Murmur2.hash(value, value.length, 0);
                if (inputHash != generatedHash) {
                    System.out.println("different has for user id " + userId + ". Input hash = " + inputHash + " generated hash = " + generatedHash);
                }
                line = reader.readLine();
            }
            System.out.println("DONE");
        }catch (FileNotFoundException ex) {
            Logger.getLogger(MurmurHashTest.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MurmurHashTest.class.getName()).log(Level.SEVERE, null, ex);

        } finally {
            try {
                reader.close();
                fis.close();
            } catch (IOException ex) {
                Logger.getLogger(MurmurHashTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
