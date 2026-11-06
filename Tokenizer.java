import java.text.Normalizer;
import java.util.*;

public class Tokenizer {

    private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
        "de","la","que","el","en","y","a","los","del","se","las","por","un","para",
        "con","no","una","su","al","lo","como","mas","pero","sus","le","ya","o",
        "este","si","porque","esta","entre","cuando","muy","sin","sobre","tambien",
        "me","hasta","hay","donde","quien","desde","todo","nos","durante","todos",
        "uno","les","ni","contra","otros","ese","eso","ante","ellos","esto","mi",
        "antes","algunos","unos","otro","otra","otras","tanto","esa","estos","mucho",
        "nada","muchos","cual","poco","ella","estar","estas","algo","fue","ha","son",
        "es","han","ser","sera","fueron","dijo","segun","tras","sido","cada","dos"
    ));

    public static List<String> tokenize(String texto) {
        // 1. minúsculas y proteger la ñ
        String t = texto.toLowerCase().replace("ñ", "\u0000");
        // 2. quitar acentos (á -> a)
        t = Normalizer.normalize(t, Normalizer.Form.NFD).replaceAll("\\p{M}", "");
        t = t.replace("\u0000", "ñ");

        // 3. partir en palabras y filtrar
        List<String> tokens = new ArrayList<>();
        for (String tok : t.split("[^a-zñ]+")) {
            if (tok.length() > 2 && !STOPWORDS.contains(tok)) {
                tokens.add(tok);
            }
        }
        return tokens;
    }
}