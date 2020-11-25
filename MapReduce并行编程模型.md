# MapReduce并行编程模型


## 一、课前准备

1.  准备3节点hadoop集群
2. 安装IDEA编程工具
3.  安装maven并配置环境变量

## 二、课堂主题

1. 围绕MapReduce分布式计算讲解


## 三、课堂目标

1.  理解MapReduce编程模型
2. 独立完成一个MapReduce程序并运行成功
3. 了解MapReduce工程流程
4. 掌握并描述出shuffle全过程（面试）
5. 理解并解决数据倾斜

## 四、知识要点

### 1. MapReduce编程模型（10分钟）

![](assets/Image201906191834-1562922704761.png)

- MapReduce是采用一种**分而治之**的思想设计出来的分布式计算框架
- 如一复杂或计算量大的任务，单台服务器无法胜任时，可将此大任务切分成一个个小的任务，小任务分别在不同的服务器上**并行**的执行；最终再汇总每个小任务的结果
- MapReduce由两个阶段组 成：Map阶段（切分成一个个小的任务）、Reduce阶段（汇总小任务的结果）。

![](assets/Image201906251747.png)

#### 1.1 Map阶段

- map()函数的输入是kv对，输出是一系列kv对，输出写入**本地磁盘**。

#### 1.2 Reduce阶段

- reduce()函数的输入是kv对（即map的输出（kv对））；输出是一系列kv对，最终写入HDFS

![](assets/Image201906251807.png)

#### 1.3 Main程序入口



### 2. MapReduce编程示例

- 以**词频统计**为例：统计一批英文文章当中，各个单词出现的总次数

#### 2.1 MapReduce原理图（10分钟）

![](assets/Image201906271715.png)

- block对应一个分片split，一个split对应一个map task
- reduce task的个数由程序中编程指定

#### 2.2 MR参考代码（10分钟）

**2.2.1 Mapper代码**

```java
package com.kaikeba.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String word : words) {
            // 每个单词出现１次，作为中间结果输出
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
```

**2.2.2 Reducer代码**

```java
package com.kaikeba.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    /*
        key: hello
        value: List(1, 1, ...)
    */
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable count : values) {
            sum = sum + count.get();
        }
        context.write(key, new IntWritable(sum));// 输出最终结果
    }
}
```

**2.2.3 Main程序入口**

```java
package com.kaikeba.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordMain {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, WordMain.class.getSimpleName());

        // 打jar包
        job.setJarByClass(WordMain.class);

        // 通过job设置输入/输出格式
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置处理Map/Reduce阶段的类
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);
        //如果map、reduce的输出的kv对类型一致，直接设置reduce的输出的kv对就行；如果不一样，需要分别设置map, reduce的输出的kv类型
        //job.setMapOutputKeyClass(.class)
        // 设置最终输出key/value的类型m
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 提交作业
        job.waitForCompletion(true);
    }
}
```

#### 2.3 本地运行（5分钟）

![](assets/Image201906271936.png)

![](assets/Image201906271933.png)

![](assets/Image201906271935.png)

#### 2.4 集群运行（5分钟）

有两种方式

**2.4.1 方式一**

![](assets/Image201906271945.png)

![](assets/Image201906271941.png)

![](assets/Image201906271939.png)

![](assets/Image201906271937.png)

鼠标右键->run运行

![](assets/Image201906271943.png)

**2.4.2 方式二**

用maven将项目打包

用hadoop jar命令行运行mr程序

```shell
[bruce@node-01 Desktop]$ hadoop jar com.kaikeba.hadoop-1.0-SNAPSHOT.jar com.kaikeba.hadoop.wordcount.WordCountMain /NOTICE.txt /wordcount01
```

![](assets/wordcount.gif)

### 3. WEB UI查看结果（5分钟）

#### 3.1 Yarn

浏览器url地址：rm节点IP:18088

![](assets/Image201906272001.png)

#### 3.2 HDFS结果

![](assets/Image201906272002.png)

### 4. Combiner（15分钟）

- map端本地聚合
- **不论运行多少次Combine操作，都不会影响最终的结果**
- 并非所有的mr都适合combine操作，比如求平均值

![](assets/Image201906261032.png)

- WordCountMap与WordCountReduce代码不变
- WordCountMain中，增加job.**setCombinerClass**(WordCountReduce.class);
- 详见工程代码

```java
package com.kaikeba.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountMain {
    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        if (args.length != 2 || args == null) {
            System.out.println("please input Path!");
            System.exit(0);
        }

        Configuration configuration = new Configuration();
        //configuration.set("mapreduce.job.jar","/home/bruce/project/kkbhdp01/target/com.kaikeba.hadoop-1.0-SNAPSHOT.jar");

        Job job = Job.getInstance(configuration, WordCountMain.class.getSimpleName());

        // 打jar包
        job.setJarByClass(WordCountMain.class);

        // 通过job设置输入/输出格式
        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入/输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置处理Map/Reduce阶段的类
        job.setMapperClass(WordCountMap.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);
        //如果map、reduce的输出的kv对类型一致，直接设置reduce的输出的kv对就行；如果不一样，需要分别设置map, reduce的输出的kv类型
        //job.setMapOutputKeyClass(.class)
        // 设置最终输出key/value的类型m
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 提交作业
        job.waitForCompletion(true);

    }
}
```

![](assets/Image201906272006.png)

### 5. Shuffle（20分钟）

![](assets/Image201905231409.png)

![](assets/Image201906272049.png)

![](assets/Image201906280906.png)

### 6. 自定义分区Partition（15分钟）

- MapReduce自带的分区器是**HashPartitioner**
- 原理：先对map输出的key求hash值，再模上reduce task个数，根据结果，决定此输出kv对，被匹配的reduce取走

![](assets/Image201906280826.png)

![](assets/Image201906272145.png)

- 根据业务逻辑，设计自定义分区，比如实现图上的功能

#### 6.1 默认分区

- 读取文件customPartition.txt，内容如下：

  ```
  Dear River
  Dear River Bear Car
  Car Dear Car Bear Car
  Dear Car Bear Car
  ```

- 默认HashPartitioner分区时，查看结果（看代码）

![](assets/Image201906272204.png)

结果如下：

![](assets/Image201906272210.png)

#### 6.2 自定义分区

- 现开户自定义分区功能，并**设定reduce个数**为4
- 详见工程代码

```java
package com.kaikeba.hadoop.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class CustomPartitioner extends Partitioner<Text, IntWritable> {
    public static HashMap<String, Integer> dict = new HashMap<String, Integer>();

    static{
        dict.put("Dear", 0);
        dict.put("Bear", 1);
        dict.put("River", 2);
        dict.put("Car", 3);
    }

    public int getPartition(Text text, IntWritable intWritable, int i) {
        //
        int partitionIndex = dict.get(text.toString());
        return partitionIndex;
    }
}
```

![](assets/Image201906272213.png)

- 运行结果

![](assets/Image201906272217.png)

### 7. 二次排序（15分钟）

- MapReduce中，根据key进行分区、排序、分组
- MapReduce会按照基本类型对应的key进行排序，如int类型的IntWritable，默认升序排序
- 为什么要自定义排序规则？
- 现有需求，需要自定义key类型，并自定义key的排序规则，如按照人的salary降序排序，若相同，则再按age升序排序；若salary、age相同，则放入同一组
- 详见工程代码

```java
package com.kaikeba.hadoop.secondarysort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
tom	20	8000
nancy	22	8000
ketty	22	9000
stone	19	10000
green	19	11000
white	30	29000
socrates	29	40000
*/
public class Person implements WritableComparable<Person> {
    private String name;
    private int age;
    private int salary;

    public Person() {
    }

    public Person(String name, int age, int salary) {
        //super();
        this.name = name;
        this.age = age;
        this.salary = salary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return this.salary + "  " + this.age + "    " + this.name;
    }

    //先比较salary，高的排序在前；若相同，age小的在前
    public int compareTo(Person o) {
        int compareResult1= this.salary - o.salary;
        if(compareResult1 != 0) {
            return -compareResult1;
        } else {
            return this.age - o.age;
        }
    }

    //序列化，将NewKey转化成使用流传送的二进制
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(age);
        dataOutput.writeInt(salary);
    }

    //使用in读字段的顺序，要与write方法中写的顺序保持一致
    public void readFields(DataInput dataInput) throws IOException {
        //read string
        this.name = dataInput.readUTF();
        this.age = dataInput.readInt();
        this.salary = dataInput.readInt();
    }
}
```



### 8. MapReduce分区倾斜

什么是数据倾斜？

数据中不可避免地会出现离群值（outlier），并导致数据倾斜。这些离群值会显著地拖慢MapReduce的执行。常见的数据倾斜有以下几类：

1. 数据频率倾斜——某一个区域的数据量要远远大于其他区域。比如某一个key对应的键值对远远大于其他键的键值对。

2. 数据大小倾斜——部分记录的大小远远大于平均值。



在map端和reduce端都有可能发生数据倾斜。在reduce端的数据倾斜常常来源于MapReduce的默认分区器。

数据倾斜会导致map和reduce的任务执行时间大为延长，也会让需要缓存数据集的操作消耗更多的内存资源。

#### 8.1 如何诊断是否存在数据倾斜（10分钟）

1. 关注由map的输出数据中的数据频率倾斜的问题。

2. 如何诊断map输出中哪些键存在数据倾斜？
   - 在reduce方法中加入记录map输出键的详细情况的功能
   
   - 发现倾斜数据之后，有必要诊断造成数据倾斜的那些键。有一个简便方法就是在代码里实现追踪每个键的**最大值**。为了减少追踪量，可以设置数据量阀值，只追踪那些数据量大于阀值的键，并输出到日志中。实现代码如下
   
```java
   package com.kaikeba.hadoop.dataskew;
   
   import org.apache.hadoop.io.IntWritable;
   import org.apache.hadoop.io.Text;
   import org.apache.hadoop.mapreduce.Reducer;
   import org.apache.log4j.Logger;
   
   import java.io.IOException;
   
   public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
   
       private int maxValueThreshold;
   
       //日志类
       private static final Logger LOGGER = Logger.getLogger(WordCountReduce.class);
   
       @Override
       protected void setup(Context context) throws IOException, InterruptedException {
   
           //一个键达到多少后，会做数据倾斜记录
           maxValueThreshold = 10000;
       }
   
       /*
               (hello, 1)
               (hello, 1)
               (hello, 1)
               ...
               (spark, 1)
   
               key: hello
               value: List(1, 1, 1)
           */
       public void reduce(Text key, Iterable<IntWritable> values,
                             Context context) throws IOException, InterruptedException {
           int sum = 0;
           //用于记录键出现的次数
           int i = 0;
   
           for (IntWritable count : values) {
               sum += count.get();
               i++;
           }
   
           //如果当前键超过10000个，则打印日志
           if(i > maxValueThreshold) {
               LOGGER.info("Received " + i + " values for key " + key);
           }
   
           context.write(key, new IntWritable(sum));// 输出最终结果
       };
   }
```

   - 运行作业后就可以从日志中判断发生倾斜的键以及倾斜程度；跟踪倾斜数据是了解数据的重要一步，也是设计MapReduce作业的重要基础


#### 8.2 减缓Reduce数据倾斜（30分钟）

1. Reduce数据倾斜一般是指map的输出数据中存在数据频率倾斜的状况，即部分输出键的数据量远远大于其它的输出键

2. 如何减小reduce端数据倾斜的性能损失？常用方式有：

   1. 自定义分区

      基于输出键的背景知识进行自定义分区。例如，如果map输出键的单词来源于一本书。其中大部分必然是省略词（stopword）。那么就可以将自定义分区将这部分省略词发送给固定的一部分reduce实例。而将其他的都发送给剩余的reduce实例。

   2. Combine

      使用Combine可以大量地减小数据频率倾斜和数据大小倾斜。在可能的情况下，combine的目的就是聚合并精简数据。

   3. 抽样和范围分区

      Hadoop默认的分区器是HashPartitioner，基于map输出键的哈希值分区。这仅在数据分布比较均匀时比较好。在有数据倾斜时就很有问题。

      使用分区器需要首先了解数据的特性。**TotalOrderPartitioner**中，可以通过对原始数据进行抽样得到的结果集来预设分区边界值。TotalOrderPartitioner中的范围分区器可以通过预设的分区边界值进行分区。因此它也可以很好地用在矫正数据中的部分键的数据倾斜问题。

   4. 数据大小倾斜的自定义策略

      在map端或reduce端的数据大小倾斜都会对缓存造成较大的影响，乃至导致OutOfMemoryError异常。处理这种情况并不容易。可以参考以下方法。

   - 设置mapreduce.input.linerecordreader.line.maxlength来限制RecordReader读取的最大长度。RecordReader在TextInputFormat和KeyValueTextInputFormat类中使用。默认长度没有上限。



## 五、拓展点、未来计划、行业趋势（5分钟）

1. 全排序，防止数据倾斜

   数据来源美国国家气候数据中心（NCDC）

   气候数据record的格式如下：

![](assets/Image201907151554.png)

先将数据按气温对天气数据集排序。结果存储为sequencefile文件（块压缩），气温作为输出键，数据行作为输出值

``` java
package com.kaikeba.hadoop.totalorder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;


// A MapReduce program for transforming the weather data into SequenceFile format
public class SortDataPreprocessor {
  
  static class CleanerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
  
    private NcdcRecordParser parser = new NcdcRecordParser();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      //0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF108991999999999999999999
      parser.parse(value);
      if (parser.isValidTemperature()) {
        context.write(new IntWritable(parser.getAirTemperature()), value);
      }
    }
  }


  //两个参数：/ncdc/input /ncdc/sfoutput
  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.println("<input> <output>");
    }

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, SortDataPreprocessor.class.getSimpleName());
    job.setJarByClass(SortDataPreprocessor.class);
    //
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(CleanerMapper.class);
    //最终输出的键、值类型
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    //reduce个数为0
    job.setNumReduceTasks(0);
    //以sequencefile的格式输出
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    //设置sequencefile的压缩、压缩算法、sequencefile文件压缩格式block
    SequenceFileOutputFormat.setCompressOutput(job, true);
    //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    //SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
```

全局排序

```java
package com.kaikeba.hadoop.totalorder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.net.URI;

//A MapReduce program for sorting a SequenceFile with IntWritable keys using the TotalOrderPartitioner to globally sort the data
public class SortByTemperatureUsingTotalOrderPartitioner{

  //两个参数：/ncdc/sfoutput /ncdc/totalorder
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("<input> <output>");
    }

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, SortByTemperatureUsingTotalOrderPartitioner.class.getSimpleName());
    job.setJarByClass(SortByTemperatureUsingTotalOrderPartitioner.class);
    //
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    SequenceFileOutputFormat.setCompressOutput(job, true);
    //SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

    //
    job.setNumReduceTasks(3);

    //分区器
    job.setPartitionerClass(TotalOrderPartitioner.class);

    //每一个参数：采样率；第二个参数：最大样本数；第三个参数：最大分区数；三者任一满足，就停止采样
    InputSampler.Sampler<IntWritable, Text> sampler =
            new InputSampler.RandomSampler<IntWritable, Text>(0.1, 10000, 10);

    InputSampler.writePartitionFile(job, sampler);

    // Add to DistributedCache
    String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
    URI partitionUri = new URI(partitionFile);
    //添加到分布式缓存中
    job.addCacheFile(partitionUri);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
```



## 六、总结（5分钟）

![](assets/Image201907152013.png)


## 七、作业

可从下面的题库任意选择。

第3、4题

## 八、互动问答




## 九、题库 - 本堂课知识点

1. 描述MR的shuffle全流程（面试）
2. 搭建MAVEN工程，统计词频，并提交集群运行，查看结果
3. 利用搜狗数据，找出所有独立的uid并写入HDFS
4. 利用搜狗数据，找出所有独立的uid出现次数，并写入HDFS，并要求使用Map端的Combine操作
5. 谈谈什么是数据倾斜，什么情况会造成数据倾斜？（面试）
6. 对MR数据倾斜，如何解决？（面试）
   
