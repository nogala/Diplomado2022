package mx.cic;

/**
 * TODO
 * Importar bibliotecas necesarias ???
 */

import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;
import org.apache.hadoop.???;

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

    public static class Clase1 extends Mapper<Object, Text, Text, IntWritable>{
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
        public void Metodo1(Object llave, Text valor, Context cubeta) {
            StringTokenizer buscador = new StringTokenizer(valor.toString());
            while (buscador.hasMoreElements()){
                palabra.set(buscador.nextToken());
                cubeta.write(palabra,uno);
            }
        }
    }

    public static class Clase2 extends Reducer<Text , IntWritable , Text, IntWritable> {
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
        public void Metodo1(Text llaves, Iterable<IntWritable> unos, Context cubeta) {
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
    public static void main(String[] parametros){
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
            Job ?????? = Job.getInstance(configura, "Contador de Palabras");
            ??????.setJarByClass(wordcount.class);
            ??????.setMapperClass(Clase1.class);
            ??????.setCombinerClass(Clase2.class);
            ??????.setReducerClass(Clase2.class);
            ??????.setOutputKeyClass(Text.class);
            ??????.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(??????, new Path(parametros[0]));
            FileOutputFormat.setOutputPath(??????, new Path(parametros[1]));
            System.exit(??????.waitForCompletion(true) ? 0 : 1);

        }
    }
}


