legstar.avro
============

  A COBOL to [Apache Avro](http://avro.apache.org/) translator based on [LegStar](http://www.legsem.com/legstar/)

## Objectives

* Provide a Translator from a [COBOL copybook](http://en.wikipedia.org/wiki/COBOL#Data_division) to an equivalent Apache [Avro Schema](http://avro.apache.org/docs/current/#schemas).

* Provide Transformation classes to convert from mainframe data to Avro records.

  The idea is that mainframe data might be useful in environments where Apache Avro records are commonplace. On such environment is Hadoop where MapReduce jobs can read/write Avro records.

## Requirements

* Java JDK 6 and above

* Maven 3 for project build

* Ant


## Build

1. Clone the [GIT repository](https://github.com/legsem/legstar.avro.git)

2. From a command window while located in the folder where you cloned the repo, type:

>   `mvn clean install`
    
## Run the sample

  If you built the project, you will find the zip bundle under legstar.avro/legstar.avro.cob2avro/target.

  Otherwise you can get the latest released zip [here](http://search.maven.org/#search%7Cga%7C1%7Clegstar.avro.cob2avro).

  Unzip the file in a location of your choice.

  From there, go to the samples folder and type:

>   `ant`






