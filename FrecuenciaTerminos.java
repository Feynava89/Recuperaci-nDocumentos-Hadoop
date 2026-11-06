import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FrecuenciaTerminos {

    // MAPPER: "palabra@doc  conteo"  ->  (doc, "palabra=conteo")
    public static class TFMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text doc = new Text();
        private final Text valor = new Text();

        @Override
        protected void map(LongWritable offset, Text linea, Context ctx)
                throws IOException, InterruptedException {
            String[] partes = linea.toString().split("\t");   // [palabra@doc, conteo]
            if (partes.length < 2) return;

            String[] pd = partes[0].split("@");                // [palabra, doc]
            doc.set(pd[1]);
            valor.set(pd[0] + "=" + partes[1]);
            ctx.write(doc, valor);
        }
    }

    // REDUCER: junta todas las palabras de un doc y calcula TF
    public static class TFReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        private final Text clave = new Text();
        private final DoubleWritable tf = new DoubleWritable();

        @Override
        protected void reduce(Text doc, Iterable<Text> valores, Context ctx)
                throws IOException, InterruptedException {
            // Paso 1: guardar palabras y sumar el total del documento
            Map<String, Integer> conteos = new HashMap<>();
            int total = 0;
            for (Text v : valores) {
                String[] pc = v.toString().split("=");         // [palabra, conteo]
                int c = Integer.parseInt(pc[1]);
                conteos.put(pc[0], c);
                total += c;
            }
            // Paso 2: dividir cada conteo entre el total
            for (Map.Entry<String, Integer> e : conteos.entrySet()) {
                clave.set(e.getKey() + "@" + doc.toString());
                tf.set((double) e.getValue() / total);
                ctx.write(clave, tf);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "frecuencia de terminos");
        job.setJarByClass(FrecuenciaTerminos.class);
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        // el mapper emite Text,Text pero el reducer emite Text,Double
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
