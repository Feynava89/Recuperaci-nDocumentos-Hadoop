import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SimilitudCoseno {

    // ===== ETAPA 1: productos por palabra =====

    // "doc \t palabra:w,palabra:w,..."  ->  (palabra, "doc:w")
    public static class InvertirMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text palabra = new Text();
        private final Text valor = new Text();

        @Override
        protected void map(LongWritable offset, Text linea, Context ctx)
                throws IOException, InterruptedException {
            String[] partes = linea.toString().split("\t");   // [doc, vector]
            if (partes.length < 2) return;
            for (String pw : partes[1].split(",")) {
                String[] x = pw.split(":");                    // [palabra, w]
                palabra.set(x[0]);
                valor.set(partes[0] + ":" + x[1]);
                ctx.write(palabra, valor);
            }
        }
    }

    // palabra -> [docA:w, docB:w, ...]  ->  ("docA,docB", wA*wB) para cada par
    public static class ParesReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final Text par = new Text();
        private final DoubleWritable producto = new DoubleWritable();

        @Override
        protected void reduce(Text palabra, Iterable<Text> valores, Context ctx)
                throws IOException, InterruptedException {
            List<String[]> docs = new ArrayList<>();
            for (Text v : valores) docs.add(v.toString().split(":"));
            docs.sort((a, b) -> a[0].compareTo(b[0]));   // orden fijo: A,B y nunca B,A

            for (int i = 0; i < docs.size(); i++) {
                for (int j = i + 1; j < docs.size(); j++) {
                    par.set(docs.get(i)[0] + "," + docs.get(j)[0]);
                    producto.set(Double.parseDouble(docs.get(i)[1])
                               * Double.parseDouble(docs.get(j)[1]));
                    ctx.write(par, producto);
                }
            }
        }
    }

    // ===== ETAPA 2: sumar productos de cada par =====

    public static class SumaMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        private final DoubleWritable valor = new DoubleWritable();

        @Override
        protected void map(Text par, Text producto, Context ctx)
                throws IOException, InterruptedException {
            valor.set(Double.parseDouble(producto.toString()));
            ctx.write(par, valor);
        }
    }

    public static class SumaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable total = new DoubleWritable();

        @Override
        protected void reduce(Text par, Iterable<DoubleWritable> valores, Context ctx)
                throws IOException, InterruptedException {
            double suma = 0;
            for (DoubleWritable v : valores) suma += v.get();
            total.set(suma);
            ctx.write(par, total);
        }
    }

    // args: entrada(tfidf) salida
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path entrada = new Path(args[0] + "/part-r-*");   // solo vectores, no los idf-m-*
        Path temporal = new Path(args[1] + "_productos");
        Path salida = new Path(args[1]);

        Job j1 = Job.getInstance(conf, "coseno - productos");
        j1.setJarByClass(SimilitudCoseno.class);
        j1.setMapperClass(InvertirMapper.class);
        j1.setReducerClass(ParesReducer.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Text.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j1, entrada);
        FileOutputFormat.setOutputPath(j1, temporal);
        if (!j1.waitForCompletion(true)) System.exit(1);

        Job j2 = Job.getInstance(conf, "coseno - suma");
        j2.setJarByClass(SimilitudCoseno.class);
        j2.setInputFormatClass(KeyValueTextInputFormat.class);  // separa clave \t valor solo
        j2.setMapperClass(SumaMapper.class);
        j2.setCombinerClass(SumaReducer.class);
        j2.setReducerClass(SumaReducer.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(j2, temporal);
        FileOutputFormat.setOutputPath(j2, salida);
        boolean ok = j2.waitForCompletion(true);

        FileSystem.get(conf).delete(temporal, true);   // borrar intermedios
        System.exit(ok ? 0 : 1);
    }
}
