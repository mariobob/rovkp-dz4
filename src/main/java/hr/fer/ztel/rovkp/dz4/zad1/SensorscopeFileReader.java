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

    /** Gets filled with all lines from sensorscope files. */
    private Stream<String> allLines = Stream.empty();

    /**
     * Constructs a new instance of {@code SensorScopeFileReader}.
     *
     * @param inputDirectory directory with sensorscope numbered input files
     */
    public SensorscopeFileReader(Path inputDirectory) {
        this.inputDirectory = FileUtility.requireDirectory(inputDirectory);
    }

    /**
     * Returns a stream of ALL readings from all available sensorscope files from
     * the given input directory.
     */
    public Stream<SensorscopeReading> getReadingsFromSensorscopeFiles() {
        return getLinesFromSensorscopeFiles()
                .map(SensorscopeReading::parseUnchecked)
                .filter(Objects::nonNull);
    }

    /**
     * Returns a stream of ALL lines from all available sensorscope files from
     * the given input directory.
     */
    public Stream<String> getLinesFromSensorscopeFiles() {
        try (Stream<Path> files = getSensorscopeFiles()) {
            files.forEach(sensorscopeFile -> allLines = concatLinesFromFile(sensorscopeFile, allLines));
        }

        return allLines;
    }

    /**
     * Returns a stream of all available sensorscope files from the given input
     * directory.
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
     * Creates a stream of lines from the specified <tt>file</tt> and concatenates it
     * onto the given <tt>stream</tt> of lines. Returns a new stream.
     *
     * @param file file to be turned into a stream
     * @param stream existing stream with lines
     * @return a new stream
     */
    private static Stream<String> concatLinesFromFile(Path file, Stream<String> stream) {
        try {
            Stream<String> lines = Files.lines(file);
            return Stream.concat(stream, lines);
        } catch (IOException e) {
            throw new UncheckedIOException("Error occurred while reading lines from " + file, e);
        }
    }

}
