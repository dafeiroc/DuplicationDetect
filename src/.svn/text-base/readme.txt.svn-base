Basiclly, the program can do such things:
1. [com.dafei.mapred] 2 map reduce tasks, one is for improved simhash algorithm , the other is for classic vsm algorithm such as pearson correlation coeffient
2. [com.dafei.algorithm] not map reduce version algorithm
3. [com.dafei.web.search]  web demo for comparison between simhash and its improvement
4. [yottabox] yotta-based, hbase-oriented REST storage service
5. [com.dafei.retargeting.servlet] redundant things please ignore

DOS and DONTS
1.u need to install <hadoop>,<hbase>,[hive],[thrift] on your clusters , and set all environment in your bashrc or bashprofile
please refer the hadoop wiki for more information

2. u need to copy the /etc/hosts to your host machine, if you are running on windows,it seems "%system32%/drivers/etc/hosts"

3.u need to create hive table "terms","metadatas","term_and_resource" ,please refer the jdbc:db2://192.168.14.159:50000/yotta863
to find the type requirement(related table in db2 are: db2admin.terms, db2admin.metadata, db2admin.term_and_resource_no_duplication)

4.create hbase table (many-to-many case):
term, metadta

5. As for running hadoop job, reduce io in map task and reduce, just use them for computing, DONT connect DB, DONT connect FileSystem if applicable
 to run com.dafei.mapred.simhash.SimHashMapRed or com.dafei.mapred.vsm.VSMMap , u need to pre process first!(merging sequence file first!)

TIPS:
1.if you wanna to run com.dafei.datasource.Dump2HbaseMain, u need to change com.dafei.config.ConnectionFactory file(line:26) to the path which indicates
the config.properties e.g. "e:\\config.properties." because it need to find hbase configuration file

2. the environment variable would probably like this in .bashrc which indicates the hadoop,hive,hbase,mahout and maven:

export MAVEN_OPTS="-Xmx1024m"
export HADOOP_HOME="/hadoop/hadoop-0.20.0"
export HIVE_HOME="/hive-0.6.0"
export HBASE_HOME="/hbase-0.20.6"
export MAVEN_HOME="/home/xjtudlc/apache-maven-2.2.1"
export MAHOUT_HOME="/home/xjtudlc/mahout-distribution-0.4"
export PATH="$MAHOUT_HOME/bin:$HBASE_HOME/bin:$MAVEN_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH"

3. to use hive, u must set HADOOP_HOME environment.
4. after successfully install thrift, u need to run "setup.py" under "$thrift_direcotry/lib/py" to import all thrift library to ur system
5. to run yottabox ,
 1, mvn assembly:assembly
 2, copy jersey-json-1.1.5.1.jar and target/DuplicationDection-1.0-SNAPSHOT-jar-with-dependencies.jar to $HBASE_HOME/lib
 3, $HBASE_HOME/bin/hbase yottabox.Main listening on port 9527

6. how to debug mapper and reduce
   USE log4j, then see logs in the task tracker.