package mx.cic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;
/**
 * Nombrar las clases correspondentes a la acción que realiza
 * Clase1, Clase2
 * Nombrar los metodos
 * Metodo1, Metodo2
 * Documentar el programa
 *
 */


public class wordcount {

    public static class Clase1 extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable uno = new IntWritable(1);
        private Text palabra = new Text();

        /**
         * TODO
         * @param llave
         * @param valor
         * @param cubeta
         * @throws java.io.IOException
         * @throws InterruptedException
         */
        public void Metodo1(Object llave, Text valor, Context cubeta) throws IOException, InterruptedException {
            StringTokenizer buscador = new StringTokenizer(valor.toString());
            while (buscador.hasMoreElements()){
                palabra.set(buscador.nextToken());
                cubeta.write(palabra,uno);
            }
        }
    }

    public static class Clase2 extends Reducer<Text , IntWritable , Text, IntWritable> {
        private IntWritable resultado = new IntWritable();
        /**
         * @param llaves
         * @param unos
         * @param cubeta
         */
        public void Metodo1(Text llaves, Iterable<IntWritable> unos, Context cubeta) throws IOException, InterruptedException {
            int suma = 0;
            for (IntWritable valor:
                    unos) {
                suma += valor.get();
            }
            resultado.set(suma);
            cubeta.write(llaves, resultado);
        }
    }

    /**
     * @param parametros
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] parametros) throws IOException, InterruptedException, ClassNotFoundException {
        if (parametros.length == 0){
            System.err.println("Usage: ... WordCount_1 <input source> <output dir>");
            System.exit(1);
        }
        else {
            /**
             * Comletar el codigo (job)
             */
            Configuration configura = new Configuration();
            Job job = Job.getInstance(configura, "Contador de Palabras");
            job.setJarByClass(wordcount.class);
            job.setMapperClass(Clase1.class);
            job.setCombinerClass(Clase2.class);
            job.setReducerClass(Clase2.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(parametros[0]));
            FileOutputFormat.setOutputPath(job, new Path(parametros[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
    }
}


