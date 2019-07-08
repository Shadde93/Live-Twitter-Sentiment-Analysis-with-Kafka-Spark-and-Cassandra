#export PYTHONPATH=""
export CASSANDRA_HOME="/home/server/KTH/DIC/lab3/cassandra"
export JAVA_HOME="/home/server/KTH/DIC/lab1/jdk1.8"
export KAFKA_HOME="/home/server/KTH/DIC/lab3/kafka"

export PYSPARK_DRIVER_PYTHON=python3
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_PYTHONPATH=$PYSPARK_PYTHON
export HADOOP_HOME="/home/server/KTH/DIC/lab1/hadoop"

export PATH=$KAFKA_HOME/bin:$PATH
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
