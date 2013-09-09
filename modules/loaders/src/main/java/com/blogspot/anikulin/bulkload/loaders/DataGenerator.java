package com.blogspot.anikulin.bulkload.loaders;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 *
 * Generates Tab Separated files with test data
 */
public class DataGenerator {

    private static final String ROW_DATA_FILE = "row_data.txt";

    public static void main( String[] args )
    {
        if (args.length < 2) {
            System.out.println("Wrong input parameters. Use generator [fileName] [rowsCount] optional: [splitByCount]");
            return;
        }

        String outputFile = args[0];
        long rowsCount = Long.parseLong(args[1]);
        long splitRowsCount = args.length == 3 ? Long.parseLong(args[2]) : rowsCount;

        InputStream inputStream = null;
        StringWriter resourceWriter = null;
        BufferedWriter fileWriter = null;

        try {
            inputStream = DataGenerator.class.getClassLoader().getResourceAsStream(ROW_DATA_FILE);

            resourceWriter = new StringWriter();
            IOUtils.copy(inputStream, resourceWriter);
            String rowString = resourceWriter.toString();

            System.out.println("Start");

            int counter = 0;
            for (long i = 0; i < rowsCount; i++) {

                if (i % splitRowsCount == 0) {
                    IOUtils.closeQuietly(fileWriter);
                    fileWriter = createFile(outputFile, counter++);
                }

                fileWriter.write(
                        String.format("%d\t%s\n", i, rowString)
                );
            }
            System.out.println("Successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(fileWriter);
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(resourceWriter);
        }
    }


    private static BufferedWriter createFile(String filePath, int counter) throws IOException {
        String newFileName = String.format(
                "%s%d.%s",
                FilenameUtils.removeExtension(filePath),
                counter,
                FilenameUtils.getExtension(filePath)
        );

        return new BufferedWriter(new FileWriter(newFileName));
    }
}
