package com.blogspot.anikulin.bulkload.loaders;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.io.StringWriter;

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
        long  rowsCount = Long.parseLong(args[1]);
        long splitCount = args.length == 3 ? Long.parseLong(args[2]) : rowsCount;

        InputStream inputStream = null;
        StringWriter writer = null;

        try {
            inputStream = DataGenerator.class.getClassLoader().getResourceAsStream(ROW_DATA_FILE);

            writer = new StringWriter();
            IOUtils.copy(inputStream, writer);
            String rowString = writer.toString();

            System.out.println("Start");

            StringBuilder builder = new StringBuilder();

            int counter = 0;
            for (long i = 0; i < rowsCount; i++) {
                builder.append(
                        String.format("%d\t%s\n", i, rowString)
                );

                if (((i+1) % splitCount == 0) || ((i+1) == rowsCount)) {
                    FileUtils.writeStringToFile(
                           new File(getFileName(outputFile, counter++)),
                           builder.toString()
                    );
                    builder = new StringBuilder();
                }
            }

            System.out.println("Successfully!");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
            IOUtils.closeQuietly(writer);
        }
    }

    private static String getFileName(String file, int counter) {
        return String.format("%s%d.%s",
                FilenameUtils.removeExtension(file),
                counter,
                FilenameUtils.getExtension(file)
        );
    }
}
