package hr.fer.ztel.rovkp.dz4.zad1;

import hr.fer.ztel.rovkp.dz4.util.FileUtility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SensorscopeFileReader {

    /** Regex defining sensorscope monitor files. */
    private static final Pattern FILENAME_REGEX = Pattern.compile("sensorscope-monitor-(\\d+)\\.txt");

    /** Directory with sensorscope numbered input files. */
    private final Path inputDirectory;

    /**
     * Constructs a new instance of {@code SensorScopeFileReader}.
     *
     * @param inputDirectory directory with sensorscope numbered input files
     * @throws IllegalArgumentException if path does not exist or is not a directory
     */
    public SensorscopeFileReader(Path inputDirectory) {
        this.inputDirectory = FileUtility.requireDirectory(inputDirectory);
    }

    /**
     * Returns a stream of ALL readings from all available sensorscope files from
     * the given input directory.
     *
     * @throws UncheckedIOException if an I/O error occurs
     */
    public Stream<SensorscopeReading> getReadingsFromSensorscopeFiles() {
        return getLinesFromSensorscopeFiles()
                .map(SensorscopeReading::parseUnchecked)
                .filter(Objects::nonNull);
    }

    /**
     * Returns a stream of ALL lines from all available sensorscope files from
     * the given input directory.
     *
     * @throws UncheckedIOException if an I/O error occurs
     */
    public Stream<String> getLinesFromSensorscopeFiles() {
        return getSensorscopeFiles()
                .flatMap(SensorscopeFileReader::lines);
    }

    /**
     * Returns a stream of all available sensorscope files from the given input
     * directory.
     *
     * @throws UncheckedIOException if an I/O error occurs
     */
    public Stream<Path> getSensorscopeFiles() {
        try {
            Stream<Path> files = Files.list(inputDirectory);
            return files.filter(SensorscopeFileReader::fileNameMatchesRegex);
        } catch (IOException e) {
            throw new UncheckedIOException("Error occurred while listing files in " + inputDirectory, e);
        }
    }

    /**
     * Returns true if given file name matches sensorscope file name regex.
     *
     * @param file file of which the file name is to be matched against regex
     */
    private static boolean fileNameMatchesRegex(Path file) {
        return FILENAME_REGEX.matcher(file.getFileName().toString()).matches();
    }

    /**
     * Creates a stream of lines from the specified <tt>file</tt> and returns it.
     * Throws an unchecked IO exception in case of an error.
     *
     * @param file file to be turned into a stream
     * @return a new stream
     * @throws UncheckedIOException if an I/O error occurs
     */
    private static Stream<String> lines(Path file) {
        try {
            return Files.lines(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Error occurred while reading lines from " + file, e);
        }
    }

}
