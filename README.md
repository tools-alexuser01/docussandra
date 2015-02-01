A REST-based, Document-Oriented Cassandra
=========================================

A REST API that makes Cassandra behave much like MongoDB. Stores arbitrary payloads as
BSON in Cassandra, supporting indexing, filtering, sorting, querying and pagination
via limit and offset semantics.

All with a great scale story, multi-datacenter or otherwise.

To run the project:

* Make sure Cassandra is running
* Load the schema in src/main/resources/docussandra.cql (e.g. 'cqlsh -f src/main/resources/docussandra.cql')
* mvn clean package exec:java

To create a project deployable assembly (zip file):

* mvn clean package
* mvn assembly:single

To run the project via the assembly (zip file):

* unzip 'assembly file created in above step'
* cd 'artifact sub-directory'
* java -jar 'artifact jar file' [environment name]
