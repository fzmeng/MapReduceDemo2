package hadoop.examples;

public class TableUtils {

    public static String makeString(String... cols) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            if (i > 1) {
                sb.append(',');
            }
            sb.append(cols[i]);
        }
        return sb.toString();
    }

    public static final String PERSON_TABLE_PREFIX = "p";
    public static final String SCORE_TABLE_PREFIX = "s";
}
