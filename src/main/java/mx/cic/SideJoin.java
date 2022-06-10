package mx.???;
/**
 * Todo
 * Importar las bibliotecas necesarias
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
 * Importar las bibliotecas necesarias.
 */
import java.io.???;
import java.util.???;

/**
 * Todo
 * Clase1, Clase2
 * Nombrar los metodos
 * Metodo1, Metodo2, etc.
 * Documentar el programa
 */
public class SideJoin {
    public static class Clase1 extends Mapper<Object, Text, Text, Text> {
        private Text llaveforanea = new Text();
        private Text valorforaneo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String attrs[] = value.toString().split("[\t]");
            String deptId = attrs[2];
            // The foreign join key is the dept ID
            llaveforanea.set(deptId);
            // flag this each record with prefixing it with 'A'
            valorforaneo.set("A" + value.toString());
            context.write(llaveforanea, valorforaneo);
        }
    }

    /**
     * TODO
     */
    public static class Clase2 extends Mapper<Object, Text, Text, Text> {
        private Text llaveforanea = new Text();
        private Text valorforaneo = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String attrs[] = value.toString().split("[\t]");
            String deptId = attrs[0];
            // The foreign join key is the dept ID
            llaveforanea.set(deptId);
            // flag this each record with prefixing it with 'B'
            valorforaneo.set("B" + value.toString());
            context.write(llaveforanea, valorforaneo);
        }
    }

    /**
     * TODO
     */

    public static class Clase3 extends Reducer<Text, Text, Text, Text> {
        private static final Text EMPTY_TEXT = new Text("");
        private Text tmp = new Text();
        private ArrayList<Text> listA = new ArrayList<Text>();
        private ArrayList<Text> listB = new ArrayList<Text>();
        private String joinType = null;

        /**
         * TODO
         * @param context
         */

        public void metodo1(Context context) {
            // set up join configuration based on input
            joinType = context.getConfiguration().get("join.type");
        }

        /**
         * TODO
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        public void metodo2(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear the lists
            listA.clear();
            listB.clear();
            // Put records from each table into correct lists, remove the prefix
            for (Text t : values) {
                tmp = t;
                if (tmp.charAt(0) == 'A') {
                    listA.add(new Text(tmp.toString().substring(1)));
                } else if (tmp.charAt(0) == 'B') {
                    listB.add(new Text(tmp.toString().substring(1)));
                }
            }
            // Execute joining logic based on its type
            executeJoinLogic(context);
        }

        /**
         * TODO
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        private void metodo3(Context context) throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                if (!listA.isEmpty() && !listB.isEmpty()) {
                    for (Text A : listA) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    }
                }
            }
            else if (joinType.equalsIgnoreCase("leftouter")) {
                for (Text A : listA) {
                    if (!listB.isEmpty()) {
                        for (Text B : listB) {
                            context.write(A, B);
                        }
                    } else {
                        context.write(A, EMPTY_TEXT);
                    }
                }
            }
            else if (joinType.equalsIgnoreCase("rightouter")) {
                for (Text B : listB) {
                    if (!listA.isEmpty()) {
                        for (Text A : listA) {
                            context.write(A, B);
                        }
                    } else {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            }
            else if (joinType.equalsIgnoreCase("fullouter")) {
                if (!listA.isEmpty()) {
                    for (Text A : listA) {
                        if (!listB.isEmpty()) {
                            for (Text B : listB) {
                                context.write(A, B);
                            }
                        } else {
                            context.write(A, EMPTY_TEXT);
                        }
                    }
                } else {
                    for (Text B : listB) {
                        context.write(EMPTY_TEXT, B);
                    }
                }
            }
        }
    }

    /**
     * TODO
     * @param args
     * @throws Exception
     */

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 4) {
            System.err.println("Usage: join <input-table1><input-table2><jointype:inner|leftouter|rightouter|fullouter><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Reduce Side Join");
        job.setJarByClass(SideJoin.class);
        job.setReducerClass(Clase3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Clase1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Clase2.class);
        job.getConfiguration().set("join.type", args[2]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}


