import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IndiceInvertido {

    // MAPPER: "palabra@doc  tf"  ->  (palabra, "doc:tf")
    public static class IndiceMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text palabra = new Text();
        private final Text valor = new Text();

        @Override
        protected void map(LongWritable offset, Text linea, Context ctx)
                throws IOException, InterruptedException {
            String[] partes = linea.toString().split("\t");   // [palabra@doc, tf]
            if (partes.length < 2) return;

            String[] pd = partes[0].split("@");                // [palabra, doc]
            palabra.set(pd[0]);
            valor.set(pd[1] + ":" + partes[1]);
            ctx.write(palabra, valor);
        }
    }

    // REDUCER: arma la lista de documentos, ordenada por TF descendente
    public static class IndiceReducer extends Reducer<Text, Text, Text, Text> {
        private final Text salida = new Text();

        @Override
        protected void reduce(Text palabra, Iterable<Text> valores, Context ctx)
                throws IOException, InterruptedException {
            List<String[]> docs = new ArrayList<>();
            for (Text v : valores) {
                docs.add(v.toString().split(":"));             // [doc, tf]
            }

            // ordenar de mayor a menor TF
            docs.sort((a, b) -> Double.compare(Double.parseDouble(b[1]),
                                               Double.parseDouble(a[1])));

            StringBuilder sb = new StringBuilder();
            for (String[] d : docs) {
                if (sb.length() > 0) sb.append(",");
                sb.append(d[0]).append(":").append(d[1]);
            }

            // formato: palabra \t numDocs \t doc1:tf,doc2:tf,...
            salida.set(docs.size() + "\t" + sb);
            ctx.write(palabra, salida);
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "indice invertido");
        job.setJarByClass(IndiceInvertido.class);
        job.setMapperClass(IndiceMapper.class);
        job.setReducerClass(IndiceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
