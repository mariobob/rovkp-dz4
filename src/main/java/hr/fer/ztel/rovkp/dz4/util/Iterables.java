package hr.fer.ztel.rovkp.dz4.util;

import java.util.function.Function;

public class Iterables {

    /**
     * Disable instantiation.
     */
    private Iterables() {}

    /**
     * Returns the sum of all integers in the specified iterable
     * object.
     *
     * @param integers integers to be summed
     * @return sum of all integers
     */
    public static int sum(Iterable<Integer> integers) {
        int sum = 0;
        for (int i : integers) {
            sum += i;
        }
        return sum;
    }

    /**
     * Returns the sum of all integer values from <tt>elements</tt>,
     * extracted by the specified key extractor function.
     *
     * @param elements elements to be summed
     * @param keyExtractor integer extractor function
     * @param <T> element type
     * @return sum of all integer values
     */
    public static <T> int sum(Iterable<T> elements, Function<? super T, Integer> keyExtractor) {
        int sum = 0;
        for (T value : elements) {
            sum += keyExtractor.apply(value);
        }
        return sum;
    }

}
