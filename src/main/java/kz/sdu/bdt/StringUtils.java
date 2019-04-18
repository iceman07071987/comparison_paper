package kz.sdu.bdt;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.*;

public class StringUtils {



    public final static Set<String> STOP_WORDS = new HashSet<>(Arrays.asList("i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"));
    private static final int SHRING_SIZE = 5;

    public static String clearString(String input) {
        StringBuilder sb = new StringBuilder();
        input = input.toLowerCase();
        int l = 0;
        for (int i = 0; i < input.length();i++) {
            char ch = input.charAt(i);

            if (Character.isLetter(ch)) {
                sb.append(ch);
                l++;
            } else if (l > 0) {
                l=0;
                sb.append(" ");
            }
        }

        if (l == 0) sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    public static String createSongHashes(String lyrics) {
        StringBuilder songHashCodes = new StringBuilder();

        List<String> tokens = new ArrayList<>();

        for (String token : clearString(lyrics).split(" ")) {
            if (!STOP_WORDS.contains(token) && token.length() > 3) {
                tokens.add(token);
            }
        }

        StringBuilder shring = new StringBuilder();
        for (int i = 0; i < tokens.size() - SHRING_SIZE && i < 50;i++) {
            for (int j = 0; j < SHRING_SIZE && j < tokens.size();j++){
                shring.append(tokens.get(i + j)).append(" ");
            }
            shring.setLength(shring.length()-1);
            songHashCodes.append(shring.toString().hashCode()).append(" ");
        }

        if (songHashCodes.length() > 0) {
            songHashCodes.setLength(songHashCodes.length()-1);
        }

        return songHashCodes.toString();
    }

    public static double jaccardSimilarity(String[] h1, String[] h2) {
        int similar = 0;
        double overall = h1.length + h2.length;
        for (String hash1 : h1) {
            for (String hash2 : h2) {
                if (hash1.equals(hash2)) {
                    similar++;
                }
            }
        }

        return similar / (overall - similar);
    }
}
