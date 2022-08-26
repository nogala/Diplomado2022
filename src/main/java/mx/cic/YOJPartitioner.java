
//???;

/**
 * Todo
 * Importar las bibliotecas necesarias.
 */

/**???;
???;
???;
???;
???;
???;
???;
???;
???;
???;
???;
???;**/

/**
 * Todo
 * Importar las bibliotecas correspondientes.
 */ //???;

    public class YOJPartitioner {

        /**
         * Todo
         * Nombrar las clases correspondentes a la acción que realiza
         * Clase1, Clase2, clase3
         * Nombrar todos los metodos
         * Metodo1, Metodo2, etc.
         * Documentar el programa
         */

        public static class Clase1 extends Mapper<Object, Text, IntWritable, Text> {
            /**
             * Todo
             * @param key
             * @param value
             * @param context
             * @throws IOException
             * @throws InterruptedException
             */
            public void metodo1(Object key, Text value, Mapper<Object, Text, IntWritable, Text>.Context context)
                    throws IOException, InterruptedException {
                String words[] = value.toString().split("[|]");
                context.write(new IntWritable(Integer.parseInt(words[2])), value);

            }
        }

        public static class clase2 extends Reducer<IntWritable, Text, Text, NullWritable> {
            /**
             * Todo
             * @param key
             * @param values
             * @param context
             * @throws IOException
             * @throws InterruptedException
             */
            protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                for (Text t : values) {
                    context.write(t, NullWritable.get());
                }
            }
        }

        /**
         * Todo
         * Completar el codigo
         */

        public class clase3 extends Partitioner<IntWritable, Text> implements Configurable {

            private Configuration conf = null;

            @Override
            public int getPartition(IntWritable key, Text value, int numPartitions) {
                return key.get() % 10;
            }

            @Override
            public Configuration getConf() {
                return conf;
            }

            @Override
            public void setConf(Configuration conf) {
                this.conf = conf;
            }
        }

        /**
         * Todo
         * @param args
         * @throws Exception
         */

        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            if (args.length != 2) {
                System.err.println("Usage: YOJPartitioner <in><out>");
                System.exit(2);
            }
            /**
             * Todo
             * Completar el codigo
             */
            Job job = Job.getInstance(conf, "YOJPartitioner");
            job.setJarByClass(YOJPartitioner.class);
            job.setMapperClass(Clase1.class);
            job.setReducerClass(clase2.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            job.setPartitionerClass(clase3.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
