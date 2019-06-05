package hr.fer.ztel.rovkp.dz4.zad1;

import hr.fer.ztel.rovkp.dz4.util.FileUtility;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

// Detaljni opis 1. zadatka:
// - učitati sve ulazne datoteke u jedan stream linija: Files.list()
// - izbaciti iz streama sve linije koje se ne mogu parsirati: .filter(SensorscopeReading:isParseable)
// - sve linije mapirati u objekte razreda SensorscopeReading: .map(SensorscopeReading::parse)
// - stream sortirati po vremenima očitanja: .sorted(Comparator::comparing)
// - zapisati izlaz u CSV formatu u jedinstvenu datoteku sensorscope-monitor-all.csv
//
// Tips:
// - obrađujemo očitanja prikupljena projektom Sensorscope: https://lcav.epfl.ch/research/research-archives/research-archives-communications_and_sensor_networks_archive-html/sensorscope-en/
// - skinuti ulazne datoteke s: https://www.fer.unizg.hr/_download/repository/sensorscope-monitor.zip
// - kako učitati linije iz više datoteka u jedan Stream: https://stackoverflow.com/questions/29691209/is-there-any-way-for-reading-two-or-more-files-in-one-java8-stream

/**
 * Gets all readings from sensorscope files, sorts them by timestamp,
 * converts them to CSV format and writes them to a separate CSV file.
 */
public class Main {

    /** User home directory. For example C:\Users\Bobasti or /home/bobasti */
    private static final String HOME = System.getProperty("user.home");
    /** Directory with sensorscope numbered input files. */
    private static final Path INPUT_DIRECTORY = Paths.get(HOME, "Desktop", "sensorscope-monitor");
    /** Output file for processed sensorscope data. */
    private static final Path OUTPUT_FILE = INPUT_DIRECTORY.resolve("sensorscope-monitor-all.csv");

    /**
     * Program entry point.
     *
     * @param args input directory with sensorscope files
     *             and output csv file, both optional
     */
    public static void main(String[] args) {
        Path inputDirectory = INPUT_DIRECTORY;
        Path outputFile = OUTPUT_FILE;
        if (args.length == 2) {
            inputDirectory = Paths.get(args[0]);
            outputFile = Paths.get(args[1]);
        }

        SensorscopeFileReader reader = new SensorscopeFileReader(inputDirectory);

        try (Stream<SensorscopeReading> readings = reader.getReadingsFromSensorscopeFiles();
             PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile))) {

            readings.sorted(Comparator.comparing(SensorscopeReading::getTimeSinceEpoch))
                    .map(SensorscopeReading::toCSV)
                    .forEach(writer::println);

            printDetails(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Prints additional details to answer first assignment's questions.
     *
     * @param reader sensorscope file reader
     */
    private static void printDetails(SensorscopeFileReader reader) {
         long numFiles = reader.getSensorscopeFiles().count();
         long numLines = FileUtility.countLines(OUTPUT_FILE);
         long fileSize = FileUtility.fileSize(OUTPUT_FILE);

        System.out.println("Number of files: " + numFiles);
        System.out.println("Number of lines: " + numLines);
        System.out.print("File size: " + FileUtility.humanReadableByteCount(fileSize));
        System.out.println(" (" + fileSize + " B)");
    }

}
