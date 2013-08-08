package eu.cloudtm.wpm.sw_probe;

/**
 * Util Methods!
 *
 * @author Pedro Ruivo
 * @since 1.0
 */
public class SoftwareProbeUtil {

    public static String cleanCollection(String collectionToString) {
        if (collectionToString == null || collectionToString.isEmpty()) {
            return "null";
        }
        String result = collectionToString
                .replace("{", "")
                .replace("}", "")
                .replace(" ", "")
                .replace(",", "|");
        if (result.equals("")) {
            return "null";
        }
        return cleanValue("|" + result);
    }

    public static String cleanValue(String str) {
        if (str == null || str.isEmpty()) {
            return "null";
        }
        String ris = str.replaceAll(",", "_");
        ris = ris.replaceAll(" ", "__");
        return ris;

    }

}
