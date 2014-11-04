legstar.avro
============

A COBOL to [Apache Avro](http://avro.apache.org/) translator based on [LegStar](http://www.legsem.com/legstar/)

## Objectives

* Provide a Translator from a COBOL copybook to an equivalent Apache Avro [Schema](http://avro.apache.org/docs/current/#schemas).

* Provide Transformation classes to convert from mainframe data to Avro records.

The idea is that mainframe data might be useful in environments where Apache Avro records are commonplace. On such environment is Hadoop where MapReduce jobs can read/write Avro records.

## Requirements

* Java JDK 6 and above

* Maven 3


