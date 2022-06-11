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

public class wordcount {

    public static class Map extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable uno = new IntWritable(1);
        private final Text palabra = new Text();

        public void map(Object llave, Text valor, Context cubeta) throws IOException, InterruptedException {
            StringTokenizer buscador = new StringTokenizer(valor.toString());
            while (buscador.hasMoreElements()){
                palabra.set(buscador.nextToken());
                cubeta.write(palabra,uno);
            }
        }
    }

    public static class Reduce extends Reducer<Text , IntWritable , Text, IntWritable> {
        private final IntWritable resultado = new IntWritable();
        /**
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
            Job trabajo = Job.getInstance(configura, "Contador de Palabras");
            trabajo.setJarByClass(wordcount.class);
            trabajo.setMapperClass(Map.class);
            trabajo.setCombinerClass(Reduce.class);
            trabajo.setReducerClass(Reduce.class);
            trabajo.setOutputKeyClass(Text.class);
            trabajo.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(trabajo, new Path(parametros[0]));
            FileOutputFormat.setOutputPath(trabajo, new Path(parametros[1]));
            System.exit(trabajo.waitForCompletion(true) ? 0 : 1);

        }
    }
}


