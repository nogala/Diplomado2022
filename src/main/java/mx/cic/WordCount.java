package mx.cic;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text palabra = new Text();
      
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer iterator = new StringTokenizer(value.toString());
    	while (iterator.hasMoreTokens()) {
    		palabra.set(iterator.nextToken());
    		context.write(palabra, one);
    		}
    	}
    }
  
  public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	  private IntWritable resultado = new IntWritable();
	  
	  public void reduce(Text key, Iterable<IntWritable> values, 
			  Context context) throws IOException, InterruptedException {
		  int sum = 0;
		  for (IntWritable val : values) {
			  sum += val.get();
			  }
		  resultado.set(sum);
		  context.write(key, resultado);
		  }
	  }

  
  public static void main(String[] parametros) throws Exception {
	  if (parametros.length == 0){
          System.err.println("Usage: ... WordCount_1 <input source> <output dir>");
          System.exit(1);
      }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Contador de Palabras");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(parametros[0]));
    FileOutputFormat.setOutputPath(job,new Path(parametros[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}