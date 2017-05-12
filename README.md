# HBaseMapReduceExample

1. Import or checkout this Gradle project
2. Install HBase on your local machine (mine is Mac OS)
3. Open a terminal, type `hbase shell`, `create 'access_logs', 'details', 'page'`, then your hbase table is created, you could view here via `describe 'access_logs'`.
4. Build your shadowJar file by `gradle clean shadowJar` in the hbasemr directory, then run `java -jar build/libs/hbasemr-1.0-SNAPSHOT-all.jar`.
5. Then you could change the Main-Class from DataImporter to MRExample to run MapReduce job.