package hr.fer.ztel.rovkp.dz4.zad1.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Stream;

public class FileUtility {

    /**
     * Checks if the specified <tt>path</tt> exists . This method is designed
     * primarily for doing parameter validation in methods and constructors, as
     * demonstrated below:
     * <blockquote><pre>
     * public Foo(Path path) {
     *     this.path = Helper.requireExists(path);
     * }
     * </pre></blockquote>
     *
     * @param path path to be checked
     * @return <tt>path</tt> if it exists
     * @throws IllegalArgumentException if path does not exist
     */
    public static Path requireExists(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("The system cannot find the path specified: " + path);
        }

        return path;
    }

    /**
     * Checks if the specified <tt>path</tt> exists and is a directory. This
     * method is designed primarily for doing parameter validation in methods
     * and constructors, as demonstrated below:
     * <blockquote><pre>
     * public Foo(Path path) {
     *     this.path = Helper.requireDirectory(path);
     * }
     * </pre></blockquote>
     *
     * @param path path to be checked
     * @return <tt>path</tt> if it exists and is a directory
     * @throws IllegalArgumentException if path does not exist or is not a directory
     */
    public static Path requireDirectory(Path path) {
        requireExists(path);
        if (!Files.isDirectory(path)) {
            throw new IllegalArgumentException("The specified path must be a directory: " + path);
        }

        return path;
    }

    /**
     * Checks if the specified <tt>path</tt> exists and is a regular file.
     * This method is designed primarily for doing parameter validation in
     * methods and constructors, as demonstrated below:
     * <blockquote><pre>
     * public Foo(Path path) {
     *     this.path = Helper.requireFile(path);
     * }
     * </pre></blockquote>
     *
     * @param path path to be checked
     * @return <tt>path</tt> if it exists and is a regular file
     * @throws IllegalArgumentException if path does not exist or is not a file
     */
    public static Path requireFile(Path path) {
        requireExists(path);
        if (!Files.isRegularFile(path)) {
            throw new IllegalArgumentException("The specified path must be a file: " + path);
        }

        return path;
    }

    /**
     * Returns the number of files in the specified directory.
     *
     * @param dir directory to be listed
     * @return number of files in the directory
     * @throws UncheckedIOException if an I/O error occurs
     */
    public static long countFiles(Path dir) {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the number of lines in the specified file.
     *
     * @param file file to be read
     * @return number of lines in the file
     * @throws UncheckedIOException if an I/O error occurs
     */
    public static long countLines(Path file) {
        try (Stream<String> files = Files.lines(file)) {
            return files.count();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the size of the specified file, in bytes.
     *
     * @param file file of which size is to be determined
     * @return file size
     * @throws UncheckedIOException if an I/O error occurs
     */
    public static long fileSize(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Converts the number of bytes to a human readable byte count with binary
     * prefixes.
     *
     * @param bytes number of bytes
     * @return human readable byte count with binary prefixes
     */
    public static String humanReadableByteCount(long bytes) {
        /* Use the natural 1024 units and binary prefixes. */
        int unit = 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = "kMGTPE".charAt(exp - 1) + "i";
        return String.format(Locale.US, "%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

}
