    import java.io.*;
    import java.util.*;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.*;

public class Consulta {

    static FileSystem fs;

    // lee todas las líneas de los part-* de una carpeta de HDFS
    static List<String> leerSalida(String dir) throws IOException {
        List<String> lineas = new ArrayList<>();
        for (FileStatus st : fs.listStatus(new Path(dir))) {
            if (!st.getPath().getName().startsWith("part-")) continue;
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(st.getPath()), "UTF-8"))) {
                String l;
                while ((l = br.readLine()) != null) lineas.add(l);
            }
        }
        return lineas;
    }

    // primera línea no vacía del .txt original
    static String titulo(String doc) throws IOException {
        Path p = new Path("/practica/entrada/" + doc + ".txt");
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(p), "UTF-8"))) {
            String l;
            while ((l = br.readLine()) != null)
                if (!l.trim().isEmpty()) return l.trim();
        }
        return "(sin título)";
    }

    // vector TF-IDF normalizado de un documento
    static Map<String, Double> vector(String doc, List<String> tfidf) {
        Map<String, Double> v = new HashMap<>();
        for (String l : tfidf) {
            String[] p = l.split("\t");
            if (!p[0].equals(doc)) continue;
            for (String pw : p[1].split(",")) {
                String[] x = pw.split(":");
                v.put(x[0], Double.parseDouble(x[1]));
            }
            break;
        }
        return v;
    }

    // args: termino [k]
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: hadoop jar practica.jar Consulta <termino> [k]");
            return;
        }
        fs = FileSystem.get(new Configuration());
        int k = args.length > 1 ? Integer.parseInt(args[1]) : 5;

        List<String> tok = Tokenizer.tokenize(args[0]);
        if (tok.isEmpty()) {
            System.out.println("El término es una stopword o es muy corto.");
            return;
        }
        String termino = tok.get(0);

        // 1. documento con mayor ocurrencia del término
        String docTop = null;
        for (String l : leerSalida("/practica/indice")) {
            String[] p = l.split("\t");                       // [palabra, df, lista]
            if (p[0].equals(termino)) {
                String[] primero = p[2].split(",")[0].split(":");
                docTop = primero[0];
                System.out.println("\nTérmino: '" + termino + "' aparece en " + p[1] + " documentos");
                System.out.println("Documento con mayor ocurrencia: " + docTop + " (TF = " + primero[1] + ")");
                System.out.println("Título: " + titulo(docTop));
                break;
            }
        }
        if (docTop == null) {
            System.out.println("El término '" + termino + "' no aparece en ningún documento.");
            return;
        }

        // 2. documentos similares
        List<String[]> sims = new ArrayList<>();
        for (String l : leerSalida("/practica/similitud")) {
            String[] p = l.split("\t");                       // ["docA,docB", sim]
            String[] par = p[0].split(",");
            if (par[0].equals(docTop)) sims.add(new String[]{par[1], p[1]});
            else if (par[1].equals(docTop)) sims.add(new String[]{par[0], p[1]});
        }
        sims.sort((a, b) -> Double.compare(Double.parseDouble(b[1]), Double.parseDouble(a[1])));

        // 3. mostrar y verificar con palabras en común
        List<String> tfidf = leerSalida("/practica/tfidf");
        Map<String, Double> vA = vector(docTop, tfidf);

        System.out.println("\n=== Top " + k + " documentos similares ===");
        for (int i = 0; i < Math.min(k, sims.size()); i++) {
            String doc = sims.get(i)[0];
            System.out.printf("%n%d. [%.4f] %s%n   %s%n", i + 1,
                    Double.parseDouble(sims.get(i)[1]), doc, titulo(doc));

            Map<String, Double> vB = vector(doc, tfidf);
            List<Map.Entry<String, Double>> comunes = new ArrayList<>();
            for (String w : vA.keySet())
                if (vB.containsKey(w))
                    comunes.add(new AbstractMap.SimpleEntry<>(w, vA.get(w) * vB.get(w)));
            comunes.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < Math.min(5, comunes.size()); j++)
                sb.append(comunes.get(j).getKey()).append(" ");
            System.out.println("   Palabras en común que más aportan: " + sb);
        }
    }
}
