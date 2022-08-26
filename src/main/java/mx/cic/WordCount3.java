package mx.cic;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Todo
 * Importar las bibliotecas correspondientes
 */

/**
 * Todo
 * Importas las bibliotecas correspondientes
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Todo
 *Nombrar las clases correspondentes a la acción que realiza
 * Clase1, Clase2
 * Nombrar los metodos
 * Metodo1, Metodo2
 * Documentar el programa
 */
public class WordCount3 {

    public static class Clase1 extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable uno = new IntWritable(1);
        private Text palabra = new Text();

        /**
         * Todo
         * @param llave
         * @param valor
         * @param contexto
         * @throws IOException
         * @throws InterruptedException
         */

        public void metodo1(Object llave, Text valor, Context contexto) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(valor.toString());
            while (itr.hasMoreElements()) {
                palabra.set(itr.nextToken());
                contexto.write(palabra, uno);
            }
        }
    }

    /**
     * Todo
     */

    public static class Clase2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private MultipleOutputs salidasm;
        private IntWritable resultado = new IntWritable();

        public void setup(Context contexto) {
            salidasm = new MultipleOutputs(contexto);
        }

        /**
         *
         * @param llave
         * @param valor
         * @param contexto
         * @throws IOException
         * @throws InterruptedException
         */

        public void metodo2(Text llave, Iterable<IntWritable> valor, Reducer<Text, IntWritable, Text, IntWritable>.Context contexto)
                throws IOException, InterruptedException {
            int suma = 0;
            for (IntWritable val : valor) {
                suma += val.get();
            }
            this.resultado.set(suma);
            salidasm.write("text", llave, this.resultado);
            salidasm.write("seq", llave, this.resultado);

            contexto.write(llave, this.resultado);
        }

        /**
         *
         * @param contexto
         * @throws IOException
         * @throws InterruptedException
         */

        public void cleanup(Context contexto) throws IOException, InterruptedException {
            salidasm.close();
        }

    }

    /**
     *
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "WordCount_1");
        job.setJarByClass(WordCount3.class);
        job.setMapperClass(Clase1.class);
        job.setCombinerClass(Clase2.class);
        job.setReducerClass(Clase2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, Text.class, IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
