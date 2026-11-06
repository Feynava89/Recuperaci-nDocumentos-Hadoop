import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BolsaPalabras {

    // MAPPER: por cada palabra emite (palabra@doc, 1)
    public static class BowMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable UNO = new IntWritable(1);
        private final Text clave = new Text();
        private String docId;

        @Override
        protected void setup(Context ctx) {
            String archivo = ((FileSplit) ctx.getInputSplit()).getPath().getName();
            docId = archivo.replace(".txt", "");   // "noticia001"
        }

        @Override
        protected void map(LongWritable offset, Text linea, Context ctx)
                throws IOException, InterruptedException {
            for (String palabra : Tokenizer.tokenize(linea.toString())) {
                clave.set(palabra + "@" + docId);
                ctx.write(clave, UNO);
            }
        }
    }

    // REDUCER: suma los 1 de cada palabra@doc
    public static class BowReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text clave, Iterable<IntWritable> valores, Context ctx)
                throws IOException, InterruptedException {
            int suma = 0;
            for (IntWritable v : valores) suma += v.get();
            total.set(suma);
            ctx.write(clave, total);
        }
    }

    // MAIN: configura y lanza el job
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance(new Configuration(), "bolsa de palabras");
        job.setJarByClass(BolsaPalabras.class);
        job.setMapperClass(BowMapper.class);
        job.setCombinerClass(BowReducer.class);
        job.setReducerClass(BowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));   // carpeta entrada
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // carpeta salida
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}