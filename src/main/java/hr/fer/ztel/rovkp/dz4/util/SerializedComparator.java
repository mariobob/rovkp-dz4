package hr.fer.ztel.rovkp.dz4.util;

import java.io.Serializable;
import java.util.Comparator;

public interface SerializedComparator<T> extends Comparator<T>, Serializable {

    static <T> SerializedComparator<T> serialize(SerializedComparator<T> comparator) {
        return comparator;
    }

}