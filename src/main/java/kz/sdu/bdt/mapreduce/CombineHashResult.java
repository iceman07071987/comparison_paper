package kz.sdu.bdt.mapreduce;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

public class CombineHashResult {

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(new FileInputStream("song_hashes.txt"));
        String br = "SECTION_BREAK";

        String line = null;
        StringBuilder buf = new StringBuilder();

        FileOutputStream fos = new FileOutputStream("song_hashes_comb.txt");

        int counter = 0;
        while(scanner.hasNextLine()) {
            counter++;
            line = scanner.nextLine();

            buf.append(line).append(br);

            if (counter % 500 == 0) {
                buf.setLength(buf.length() - br.length());
                buf.append("\n");

                fos.write(buf.toString().getBytes());

                buf.setLength(0);
            }
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length() - br.length());
            buf.append("\n");

            fos.write(buf.toString().getBytes());
        }
        fos.flush();
        fos.close();
    }
}
