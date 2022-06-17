package mx.cic;

/**
 * Luis Hermenegildo
 * Importar bibliotecas necesarias ???
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

import java.io.IOException;
import java.util.StringTokenizer;
/**
 * TODO
 * Nombrar las clases correspondentes a la acción que realiza
 * Clase1, Clase2
 * Nombrar los metodos
 * Metodo1, Metodo2
 * Documentar el programa
 *
 */

/**
 * TODO
 */
public class wordcount {

    public static class claseMap extends Mapper<Object, Text, Text, IntWritable>{
        /**
         * TODO
         */
        private final static IntWritable uno = new IntWritable(1);
        private Text palabra = new Text();

        /**
         * TODO
         * @param llave
         * @param valor
         * @param cubeta
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object llave, Text valor, Context cubeta) throws IOException, InterruptedException {
            StringTokenizer buscador = new StringTokenizer(valor.toString());
            while (buscador.hasMoreElements()){
                palabra.set(buscador.nextToken());
                cubeta.write(palabra,uno);
            }
        }
    }

    public static class claseReduce extends Reducer<Text , IntWritable , Text, IntWritable> {
        /**
         * TODO
         */
        private IntWritable resultado = new IntWritable();
        /**
         * TODO
         * @param llaves
         * @param unos
         * @param cubeta
         */
        public void reduce(Text llaves, Iterable<IntWritable> unos, Context cubeta) throws IOException, InterruptedException {
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
     * TODO
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
             * TODO
             * Comletar el codigo (??????)
             */
            Configuration configura = new Configuration();
            Job job = Job.getInstance(configura, "Contador de Palabras");
            job.setJarByClass(wordcount.class);
            job.setMapperClass(claseMap.class);
            job.setCombinerClass(claseReduce.class);
            job.setReducerClass(claseReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(parametros[0]));
            FileOutputFormat.setOutputPath(job, new Path(parametros[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
    }
}


