package ???;

/**
 * Todo
 * Importar las bibliotecas requeridas.
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

/**
 * Todo
 * Importar las bibliotecas requeridas.
 */
import java.io.???;
import java.util.???;
import java.util.???;

/**
 * Todo
 * Nombrar las clases correspondientes a la acción que realiza
 * Clase1, Clase2
 * Nombrar los metodos
 * Metodo1, Metodo2
 * Documentar el programa ???
 */
public class TopTenWords {

    public static class Clase1 extends Mapper<Object, Text, Text, IntWritable>{
        private TreeMap<Integer, String> contadorpalabraMap = new TreeMap<Integer, String>();

        public void metodo1(Object llave, Text valor, Mapper<Object, Text, Text, IntWritable>.Context contexto)
                throws IOException, InterruptedException{

            String[] palabras = valor.toString().split("[\t]");
            int contador = Integer.parseInt(palabras[1]);
            String palabra = palabras[0];
            contadorpalabraMap.put(contador, palabra);
            if (contadorpalabraMap.size()>10){
                contadorpalabraMap.remove(contadorpalabraMap.firstKey());
            }
        }

        @Override
        protected void metodo2(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : contadorpalabraMap.entrySet()) {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }

    }

    public static class clase2 extends Reducer<Text, IntWritable, Text, IntWritable>{
        private TreeMap<IntWritable, Text> contadorpalabraMap = new TreeMap<IntWritable, Text>();

        public void reduce(Text llave, Iterable<IntWritable> valores,
                           Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            for (IntWritable valor : valores) {

                contadorpalabraMap.put(valor, llave);

            }
            if (contadorpalabraMap.size() > 10) {

                contadorpalabraMap.remove(contadorpalabraMap.firstKey());
            }
            for (Map.Entry<IntWritable, Text> entry : contadorpalabraMap.descendingMap().entrySet()) {
                context.write(entry.getValue(), entry.getKey());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: toptencounter <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Top Ten Word By Occurence Counter");
        job.setJarByClass(???.class);
        job.setMapperClass(???.class);
        job.setCombinerClass(???.class);
        job.setReducerClass(???.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
