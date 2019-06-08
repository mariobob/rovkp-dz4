package hr.fer.ztel.rovkp.dz4.zad2;

import hr.fer.ztel.rovkp.dz4.util.Iterables;
import hr.fer.ztel.rovkp.dz4.util.SerializedComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

import static hr.fer.ztel.rovkp.dz4.util.SerializedComparator.serialize;

// Download StateNames.csv: https://www.fer.unizg.hr/_download/repository/StateNames.zip
/**
 * Uses Apache Spark to figure out answers to some questions
 * about names of children born in the US.
 */
public class Main {

    /** User home directory. For example C:\Users\Bobasti or /home/bobasti */
    private static final String HOME = System.getProperty("user.home");
    /** File with names sorted by state. */
    private static final Path INPUT_FILE = Paths.get(HOME, "Desktop", "StateNames", "StateNames.csv");
    /** Output file for processed name data. */
    private static final Path OUTPUT_FILE = INPUT_FILE.resolveSibling("StateNames-results.txt");

    /** Apache Spark Java RDD only accepts a serialized comparator. */
    private static final SerializedComparator<Tuple2<String, Integer>> TUPLE_COMPARING_INT = serialize((p1, p2) -> Integer.compare(p1._2, p2._2));

    /**
     * Program entry point.
     *
     * @param args input file with names of children
     *             and output csv file, both optional
     */
    public static void main(String[] args) {
        Path inputFile = INPUT_FILE;
        Path outputFile = OUTPUT_FILE;
        if (args.length == 2) {
            inputFile = Paths.get(args[0]);
            outputFile = Paths.get(args[1]);
        }

        SparkConf conf = new SparkConf().setAppName("ChildrenCount");

        // Set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create an RDD from text file lines and filter only valid records
        JavaRDD<USBabyNameRecord> records = sc.textFile(inputFile.toString())
                .map(USBabyNameRecord::parseUnchecked)
                .filter(Objects::nonNull);

        // Build the string and write it out
        String result = filterRelevantResults(records);
        System.out.println(result);
        writeToFile(result, outputFile);
    }

    /**
     * Filters records multiple times and returns a bigass string
     * consisting of an assignment questions and their answers.
     *
     * @param records records to be included in many transformations
     * @return a string consisting of multiple lines
     */
    private static String filterRelevantResults(JavaRDD<USBabyNameRecord> records) {
        StringBuilder sb = new StringBuilder();

        sb.append("1) Most unpopular male name: ");
        String mostUnpopularMaleName = records
                .filter(USBabyNameRecord::isMale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .min(TUPLE_COMPARING_INT)
                ._1();
        sb.append(mostUnpopularMaleName).append("\n\n");

        sb.append("2) 10 most popular female names: ");
        String most10PopularFemaleNames = records
                .filter(USBabyNameRecord::isFemale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .top(10, TUPLE_COMPARING_INT)
                .stream()
                .map(Tuple2::_1)
                .collect(Collectors.joining(", "));
        sb.append(most10PopularFemaleNames).append("\n\n");

        sb.append("3) State where most children were born in 1948: ");
        String stateWithMostChildrenBorn = records
                .groupBy(USBabyNameRecord::getState)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .max(TUPLE_COMPARING_INT)
                ._1();
        sb.append(stateWithMostChildrenBorn).append("\n\n");

        sb.append("4) Number of newborns throughout the years: ");
        JavaPairRDD<Integer, Integer> newbornsByYearRDD = records
                .groupBy(USBabyNameRecord::getYear)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .sortByKey();
        String numberOfNewbornsPerYear = newbornsByYearRDD
                .map(pair -> String.format("\n%d: %d", pair._1, pair._2))
                .reduce(String::concat);
        sb.append(numberOfNewbornsPerYear).append("\n\n");

        // Save these few records locally as map entries for fast search
        Map<Integer, Integer> newbornsByYearMap = newbornsByYearRDD.collectAsMap();

        sb.append("5) Percentage of name 'Lucy' throughout the years: ");
        String percentageOfNamePerYear = records
                .filter(record -> "Lucy".equals(record.getName()))
                .groupBy(USBabyNameRecord::getYear)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .sortByKey()
                .map(pair -> {
                    double percent = 100.0 * pair._2 / newbornsByYearMap.get(pair._1);
                    return String.format(Locale.US, "\n%d: %.2f", pair._1, percent);
                })
                .reduce(String::concat);
        sb.append(percentageOfNamePerYear).append("\n\n");

        sb.append("6) Total number of children born: ");
        long numChildrenBorn = newbornsByYearRDD
                .map(Tuple2::_2)
                .reduce(Integer::sum);
        sb.append(numChildrenBorn).append("\n\n");

        sb.append("7) Number of unique names: ");
        long numUniqueNames = records
                .groupBy(USBabyNameRecord::getName)
                .keys()
                .count();
        sb.append(numUniqueNames).append("\n\n");

        sb.append("8) Number of unique states: ");
        long numUniqueStates = records
                .groupBy(USBabyNameRecord::getState)
                .keys()
                .count();
        sb.append(numUniqueStates).append("\n\n");

        return sb.toString();
    }

    /**
     * Writes the specified <tt>text</tt> to results output file.
     *
     * @param text text to be written to output file
     */
    private static void writeToFile(String text, Path outputFile) {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8))) {
            writer.print(text);
        } catch (IOException e) {
            System.err.println("Error writing to file " + outputFile);
            e.printStackTrace();
        }
    }

}
