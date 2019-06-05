package hr.fer.ztel.rovkp.dz4.zad2;

import hr.fer.ztel.rovkp.dz4.util.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
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

        // TODO Do not collect immediately but use Spark's 'min' function
        sb.append("1) Most unpopular male name: ");
        String mostUnpopularMaleName = records
                .filter(USBabyNameRecord::isMale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .collect()
                .stream()
                .min(Comparator.comparingInt(Tuple2::_2))
                .get()
                ._1();
        sb.append(mostUnpopularMaleName).append("\n\n");

        sb.append("2) 10 most popular female names: ");
        String most10PopularFemaleNames = records
                .filter(USBabyNameRecord::isFemale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .collect()
                .stream()
                .sorted(Comparator.comparing(Tuple2::_2, Comparator.reverseOrder()))
                .limit(10)
                .map(Tuple2::_1)
                .collect(Collectors.joining(", "));
        sb.append(most10PopularFemaleNames).append("\n\n");

        // TODO Do not collect immediately but use Spark's 'max' function
        sb.append("3) State where most children were born in 1948: ");
        String stateWithMostChildrenBorn = records
                .groupBy(USBabyNameRecord::getState)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .collect()
                .stream()
                .max(Comparator.comparing(Tuple2::_2))
                .get()
                ._1();
        sb.append(stateWithMostChildrenBorn).append("\n\n");

        sb.append("4) Number of newborns throughout the years: ");
        List<Tuple2<Integer, Integer>> newbornsByYear = records
                .groupBy(USBabyNameRecord::getYear)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .sortByKey()
                .collect();
        newbornsByYear.forEach(pair -> sb.append("\n").append(pair._1).append(": ").append(pair._2));
        sb.append("\n\n");

        // TODO String doesn't get included
        sb.append("5) Percentage of name 'Lucy' throughout the years: ");
        records
                .filter(record -> "Lucy".equals(record.getName()))
                .groupBy(USBabyNameRecord::getYear)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .sortByKey()
                .foreach(pair -> {
                    double percent = 100.0 * pair._2 / newbornsByYear.stream().filter(p -> pair._1.equals(p._1)).findFirst().get()._2;
                    sb.append("\n").append(pair._1).append(": ").append(String.format("%.2f", percent));
                });
        sb.append("\n\n");

        sb.append("6) Total number of children born: ");
        long numChildrenBorn = newbornsByYear
                .stream()
                .mapToLong(Tuple2::_2)
                .sum();
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

        System.out.println(sb);
    }

}
