package hr.fer.ztel.rovkp.dz4.zad2;

import hr.fer.ztel.rovkp.dz4.util.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

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
    private static final Path OUTPUT_FILE = INPUT_FILE.resolveSibling("state-names.csv");

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

        // Begin building string
        StringBuilder sb = new StringBuilder();

        sb.append("1) Most unpopular male name: ");
        String mostUnpopularMaleName = records
                .filter(USBabyNameRecord::isMale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .collectAsMap()
                .entrySet()
                .stream()
                .min(Comparator.comparingInt(Map.Entry::getValue))
                .get()
                .getKey();
        sb.append(mostUnpopularMaleName).append("\n\n");

        sb.append("2) 10 most popular female names: ");
        String most10PopularFemaleNames = records
                .filter(USBabyNameRecord::isFemale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .collectAsMap()
                .entrySet()
                .stream()
                .sorted(Comparator.comparing(Map.Entry::getValue, Comparator.reverseOrder()))
                .limit(10)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(", "));
        sb.append(most10PopularFemaleNames).append("\n\n");

        sb.append("3) State where most children was born in 1948: ");
        String stateWithMostChildrenBorn = records
                .groupBy(USBabyNameRecord::getState)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .collectAsMap()
                .entrySet()
                .stream()
                .max(Comparator.comparing(Map.Entry::getValue))
                .get()
                .getKey();
        sb.append(stateWithMostChildrenBorn).append("\n\n");

        sb.append("4) Number of newborns throughout the years: ");
        // TODO 2.4.

        sb.append("5) Percentage of name 'Lucy' throughout the years: ");
        // TODO 2.5.

        sb.append("6) Total number of children born: ");
        // TODO 2.6.

        sb.append("7) Number of unique names: ");
        // TODO 2.7.

        System.out.println(sb);
    }

}
