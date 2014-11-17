legstar.avro
============

  A COBOL to [Apache Avro](http://avro.apache.org/) translator based on [LegStar](http://www.legsem.com/legstar/)

  The idea is that mainframe data might be useful in environments where Apache Avro records are commonplace. One such environment is [Hadoop](http://hadoop.apache.org/) where [MapReduce jobs](http://en.wikipedia.org/wiki/MapReduce) can read/write Avro records.

  Legstar.avro helps deliver mainframe data as Avro records.

## Objectives

* Provide a Generator to produce artifacts from a [COBOL copybook](http://en.wikipedia.org/wiki/COBOL#Data_division):

    * [Avro Schema](http://avro.apache.org/docs/current/#schemas) matching the COBOL copybook

    * [LegStar Java transformation](http://www.legsem.com/legstar/legstar-core/legstar-coxbgen/index.html) code to turn mainframe records to java beans
    
    * Avro specific records (The Generator invokes the Avro compiler for you)

* Readers for straight Java, or Hadoop, that read mainframe files and deliver an Avro record for each mainframe record

![Overview](http://legsem.github.io/images/legstar.avro.overview.png)

## Requirements

* Java JDK 6 and above (It is important that this is a [JDK](http://en.wikipedia.org/wiki/Java_Development_Kit) not a simple JRE)

* Maven 3  (If you build from sources)

* Ant 1.9.x (To run samples)

* Hadoop 2.4.1 (To run the Hadoop sample)

## Build from sources

1. Clone the [GIT repository](https://github.com/legsem/legstar.avro.git)

2. From a command window, while located in the folder where you cloned the repo, type:

>   `mvn clean install`
    
## Run the samples

  If you built the project from sources, you will find the distribution *zip* file under *legstar.avro/legstar.avro.distrib/target*.

  Otherwise you can get the latest released zip [here](http://search.maven.org/#search%7Cga%7C1%7Clegstar.avro.distrib).

  Unzip the zip file in a location of your choice.

### 1. Run the CustdatReader sample
  
  Go to the *samples* folder and type:

>   `ant`

  You can get more information on the [CustdatReader sample provided on the wiki](https://github.com/legsem/legstar.avro/wiki/CustdatReader-sample)
  
### 2. Run the CustdatHadoopReader sample
  
  Go to the *samples* folder and type:

>   `ant -f build-hadoop.xml`

  You can get more information on the [CustdatHadoopReader sample provided on the wiki](https://github.com/legsem/legstar.avro/wiki/CustdatHadoopReader-sample)







