package com.cg.javacore;

import org.junit.Test;

import java.io.*;

/**
 * @author： Cheng Guang
 * @date： 2017/4/7.
 */
public class SplitData {

    public static void main(String[] args) throws Exception {
        split();
    }

    public static void split() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(new File("F:/sample_10w_201703.txt")));
        BufferedWriter writer;
        String line;

        writer = new BufferedWriter(new FileWriter("F:/sample_10w_201703_part1.txt"));
        for (int i = 0; i < 30000; i++) {
            line = reader.readLine();
            writer.write(line);
            writer.newLine();
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter("F:/sample_10w_201703_part2.txt"));
        for (int i = 0; i < 30000; i++) {
            line = reader.readLine();
            writer.write(line);
            writer.newLine();
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter("F:/sample_10w_201703_part3.txt"));
        for (int i = 0; i < 40000; i++) {
            line = reader.readLine();
            writer.write(line);
            writer.newLine();
        }
        writer.close();
    }
}
