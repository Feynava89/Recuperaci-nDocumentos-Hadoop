import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TFIDF {

    // MAPPER: "palabra \t df \t doc:tf,doc:tf,..."  ->  (doc, "palabra:tfidf")
    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int N;
        private MultipleOutputs<Text, Text> mos;
        private final Text doc = new Text();
        private final Text valor = new Text();

        @Override
        protected void setup(Context ctx) {
            N = ctx.getConfiguration().getInt("total.docs", 1);
            mos = new MultipleOutputs<>(ctx);
        }

        @Override
        protected void map(LongWritable offset, Text linea, Context ctx)
                throws IOException, InterruptedException {
            String[] partes = linea.toString().split("\t");   // [palabra, df, lista]
            if (partes.length < 3) return;

            String palabra = partes[0];
            int df = Integer.parseInt(partes[1]);
            double idf = Math.log((double) N / df);

            // evidencia del IDF en un archivo aparte
            mos.write("idf", new Text(palabra), new Text(String.valueOf(idf)));

            for (String par : partes[2].split(",")) {
                String[] dt = par.split(":");                  // [doc, tf]
                double tfidf = Double.parseDouble(dt[1]) * idf;
                if (tfidf == 0) continue;                      // palabra en TODOS los docs
                doc.set(dt[0]);
                valor.set(palabra + ":" + tfidf);
                ctx.write(doc, valor);
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            mos.close();
        }
    }

    // REDUCER: arma el vector del documento y lo normaliza
    public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
        private final Text salida = new Text();

        @Override
        protected void reduce(Text doc, Iterable<Text> valores, Context ctx)
                throws IOException, InterruptedException {
            Map<String, Double> vector = new HashMap<>();
            double suma = 0;
            for (Text v : valores) {
                String[] pw = v.toString().split(":");         // [palabra, tfidf]
                double w = Double.parseDouble(pw[1]);
                vector.put(pw[0], w);
                suma += w * w;
            }
            double norma = Math.sqrt(suma);
            if (norma == 0) return;

            // ordenado de mayor a menor peso (más fácil de leer)
            List<Map.Entry<String, Double>> lista = new ArrayList<>(vector.entrySet());
            lista.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Double> e : lista) {
                if (sb.length() > 0) sb.append(",");
                sb.append(e.getKey()).append(":").append(e.getValue() / norma);
            }
            salida.set(sb.toString());
            ctx.write(doc, salida);
        }
    }

    // args: entrada salida N
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("total.docs", Integer.parseInt(args[2]));  // ANTES de crear el Job

        Job job = Job.getInstance(conf, "tf-idf");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDFMapper.class);
        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleOutputs.addNamedOutput(job, "idf", TextOutputFormat.class, Text.class, Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
