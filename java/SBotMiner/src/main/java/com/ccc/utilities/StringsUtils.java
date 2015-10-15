package com.ccc.utilities;

import java.util.ArrayList;

/**
 *
 * @author Tom
 */
public class StringsUtils {

    public static String join(ArrayList<?> list, int initialCapacity) {
        if (list == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(initialCapacity);
        sb.append('[');
        for (int i = 0;; i++) {
            sb.append(list.get(i)).toString();
            if (i == list.size() - 1) {
                return sb.append(']').toString();
            }
            sb.append(", ");
        }
    }

    public static String toString(Object[] objects, int initialCapacity) {
        if (objects == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(initialCapacity);
        sb.append('[');
        for (int i = 0;; i++) {
            sb.append(objects[i]).toString();
            if (i == objects.length - 1) {
                return sb.append(']').toString();
            }
            sb.append(", ");
        }
    
    }
}
