package hr.fer.ztel.rovkp.dz4.zad3;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Uses Apache Spark Streaming to fetch readings over the network.
 * For each station, it calculates the maximum solar panel current
 * in a one minute time frame.
 */
public class Main {

    /** User home directory. For example C:\Users\Bobasti or /home/bobasti */
    private static final String HOME = System.getProperty("user.home");
    /** Output file for processed sensorscope data. */
    private static final Path OUTPUT_FILE = Paths.get(HOME, "Desktop", "sensorscope-monitor", "sensorscope-monitor-network.txt");

    static {
        System.setProperty("hadoop.home.dir", "C:\\usr\\hadoop-2.8.1");
    }

    /**
     * Program entry point.
     *
     * @param args output file, optional
     */
    public static void main(String[] args) {
        Path outputFile = OUTPUT_FILE;
        if (args.length == 1) {
            outputFile = Paths.get(args[0]);
        }

        SparkConf conf = new SparkConf().setAppName("SparkStreamingMaxSolarPanelCurrent");

        // Set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            // Spark streaming application requires at least 2 threads
            conf.setMaster("local[2]");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Create a stream from text records and filter only valid records
        JavaDStream<SensorscopeReading> records = jssc.socketTextStream("localhost", SensorStreamGenerator.PORT)
                .map(SensorscopeReading::parseUnchecked)
                .filter(Objects::nonNull);

        // Do the job
        // TODO Doesn't work. Make this right.
        JavaPairDStream<Long, Double> result = records
                .mapToPair(reading -> new Tuple2<>(reading.getStationID(), reading.getSolarPanelCurrent()))
                .reduceByKey(Double::max);

        // Save aggregated tuples to text file
        result.dstream().saveAsTextFiles(outputFile.toString(), "txt");

        // Start the streaming context and wait for it to "finish"
        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
