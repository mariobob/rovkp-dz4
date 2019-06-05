package hr.fer.ztel.rovkp.dz4.zad2;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Comparator;

/**
 * US baby name record. Contains:
 * <ol>
 *     <li>ID</li>
 *     <li>Name</li>
 *     <li>Year</li>
 *     <li>Gender</li>
 *     <li>State</li>
 *     <li>Count</li>
 * </ol>
 */
@Value
@Builder
public class USBabyNameRecord implements Serializable {

    /** Compares {@code USBabyNameRecord} objects by count. */
    public static final Comparator<USBabyNameRecord> COUNT_COMPARATOR = Comparator.comparingInt(USBabyNameRecord::getCount);

    private final int id;
    private final String name;
    private final int year;
    private final String gender;
    private final String state;
    private final int count;

    /**
     * Parses the given string as a US baby name record.
     *
     * Throws an exception if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code USBabyNameRecord} object
     * @throws IllegalArgumentException if string can not be parsed
     */
    public static USBabyNameRecord parse(String s) {
        try {
            String[] args = s.split(",");
            if (!(args[3].equals("M") || args[3].equals("F"))) {
                throw new IllegalArgumentException("Gender must be either M or F.");
            }

            return builder()
                    .id(Integer.parseInt(args[0]))
                    .name(args[1])
                    .year(Integer.parseInt(args[2]))
                    .gender(args[3])
                    .state(args[4])
                    .count(Integer.parseInt(args[5]))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s, e);
        }
    }

    /**
     * Parses the given string as a US baby name record.
     *
     * Instead of throwing an exception, this method returns <tt>null</tt>
     * if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code USBabyNameRecord} object or <tt>null</tt>
     */
    public static USBabyNameRecord parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns true if record shows a male name.
     *
     * @return true if record shows a male name
     */
    public boolean isMale() {
        return "M".equals(gender);
    }

    /**
     * Returns true if record shows a female name.
     *
     * @return true if record shows a female name
     */
    public boolean isFemale() {
        return "F".equals(gender);
    }

}
