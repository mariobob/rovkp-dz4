package hr.fer.ztel.rovkp.dz4.zad1;

import lombok.Builder;
import lombok.Value;

import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A sensorscope reading. Contains:
 * <ol>
 *     <li>Station ID</li>
 *     <li>Year</li>
 *     <li>Month</li>
 *     <li>Day</li>
 *     <li>Hour</li>
 *     <li>Minute</li>
 *     <li>Second</li>
 *     <li>Time since the epoch [s]</li>
 *     <li>Sequence Number</li>
 *     <li>Config Sampling Time [s]</li>
 *     <li>Data Sampling Time [s]</li>
 *     <li>Radio Duty Cycle [%]</li>
 *     <li>Radio Transmission Power [dBm]</li>
 *     <li>Radio Transmission Frequency [MHz]</li>
 *     <li>Primary Buffer Voltage [V]</li>
 *     <li>Secondary Buffer Voltage [V]</li>
 *     <li>Solar Panel Current [mA]</li>
 *     <li>Global Current [mA]</li>
 *     <li>Energy Source</li>
 * </ol>
 *
 * <b>Note</b>: All time units are in seconds.
 */
@Value
@Builder
public class SensorscopeReading implements Comparable<SensorscopeReading> {

    /** Compares {@code SensorscopeReading} objects by time since epoch. */
    public static final Comparator<SensorscopeReading> EPOCH_COMPARATOR = Comparator.comparingLong(SensorscopeReading::getTimeSinceEpoch);

    private final long stationID;
    private final int year;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final int second;
    private final long timeSinceEpoch;
    private final long sequenceNumber;
    private final double configSamplingTime;
    private final double dataSamplingTime;
    private final double radioDutyCycle;
    private final double radioTransmissionPower;
    private final double radioTransmissionFrequency;
    private final double primaryBufferVoltage;
    private final double secondaryBufferVoltage;
    private final double solarPanelCurrent;
    private final double globalCurrent;
    private final double energySource;

    /**
     * Parses the given string as a sensorscope reading object.
     *
     * Throws an exception if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code SensorscopeReading} object
     * @throws IllegalArgumentException if string can not be parsed
     */
    public static SensorscopeReading parse(String s) {
        try {
            String[] args = s.split(" ");
            return builder()
                    .stationID(Long.parseLong(args[0]))
                    .year(Integer.parseInt(args[1]))
                    .month(Integer.parseInt(args[2]))
                    .day(Integer.parseInt(args[3]))
                    .hour(Integer.parseInt(args[4]))
                    .minute(Integer.parseInt(args[5]))
                    .second(Integer.parseInt(args[6]))
                    .timeSinceEpoch(Long.parseLong(args[7]))
                    .sequenceNumber(Long.parseLong(args[8]))
                    .configSamplingTime(Double.parseDouble(args[9]))
                    .dataSamplingTime(Double.parseDouble(args[10]))
                    .radioDutyCycle(Double.parseDouble(args[11]))
                    .radioTransmissionPower(Double.parseDouble(args[12]))
                    .radioTransmissionFrequency(Double.parseDouble(args[13]))
                    .primaryBufferVoltage(Double.parseDouble(args[14]))
                    .secondaryBufferVoltage(Double.parseDouble(args[15]))
                    .solarPanelCurrent(Double.parseDouble(args[16]))
                    .globalCurrent(Double.parseDouble(args[17]))
                    .energySource(Double.parseDouble(args[18]))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s);
        }
    }

    /**
     * Parses the given string as a sensorscope reading object.
     *
     * Instead of throwing an exception, this method returns <tt>null</tt>
     * if input can not be parsed.
     *
     * @param s string to be parsed
     * @return parsed {@code SensorscopeReading} object or <tt>null</tt>
     */
    public static SensorscopeReading parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns a CSV string from this sensorscope reading.
     *
     * @return a CSV string from this sensorscope reading
     */
    public String toCSV() {
        StringJoiner sj = new StringJoiner(",");
        sj.add(Objects.toString(stationID));
        sj.add(Objects.toString(year));
        sj.add(Objects.toString(month));
        sj.add(Objects.toString(day));
        sj.add(Objects.toString(hour));
        sj.add(Objects.toString(minute));
        sj.add(Objects.toString(second));
        sj.add(Objects.toString(timeSinceEpoch));
        sj.add(Objects.toString(sequenceNumber));
        sj.add(Objects.toString(configSamplingTime));
        sj.add(Objects.toString(dataSamplingTime));
        sj.add(Objects.toString(radioDutyCycle));
        sj.add(Objects.toString(radioTransmissionPower));
        sj.add(Objects.toString(radioTransmissionFrequency));
        sj.add(Objects.toString(primaryBufferVoltage));
        sj.add(Objects.toString(secondaryBufferVoltage));
        sj.add(Objects.toString(solarPanelCurrent));
        sj.add(Objects.toString(globalCurrent));
        sj.add(Objects.toString(energySource));
        return sj.toString();
    }

    @Override
    public int compareTo(SensorscopeReading other) {
        return EPOCH_COMPARATOR.compare(this, other);
    }

}
