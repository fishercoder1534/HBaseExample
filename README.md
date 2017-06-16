# HBaseExample

Since there are so few working Java examples on the Internet, after so many trials and error, I got HBase end-to-end working.
I think open-sourcing this project will definitely help fellow programmers.

If you find anything missing, feel free to open an issue or just send me a pull request.

What contains in this repo:
1. Use Spark to download data (Hive ORC format, csv format, etc) from S3 and generate HFiles, load HFiles into your HBase tables.
2. Or a working example of MapReduce job to load your RDD data into your HBase table.
