Use Spark to generate HFiles and then load the HFiles into HBase tables.

There are many ways to port data into HBase, here, I'm summarizing two options:
1. Use Java Client API to do PUTs into your HBase table.
2. Generate HFiles and load HFiles directly into HBase.

Approach 2 is much faster than 1. This is because HBase underlying storage format is HFile, in this case, you only need to mount the HFiles into your HTable.
Comparing with approach 1, it saves the HTTP connection overhead.

Some facts, I tested with 30+ runs for both approaches.
My data size is 17 million entries.
After fine-tuning the run parameters, approach 1 took around 10 minutes to finish.
When using approach 2, it took 4'30" to generate HFiles and less than 4 seconds to load the generated HFiles into HBase.


More on Approach 2:
1. You'll want to pre-split your HBase table into different regions when you create your HBase table. Here're the steps:
a. estimate your data size, this [post](http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/) has very detailed guide on how to this.
b. open your hbase shell in your terminal and create your hbase table: $create 'fishercoder-hbase-test', {NAME => 'fc',  COMPRESSION => 'SNAPPY'}, {SPLITS =>  ['01','02', '03', etc, '05']}

2. Build the shadowJar of HFileGenerator and scp the jar to your cluster and run the script in submit.sh from your cluster.

A bunch of gotcha moments:
1. when you encounter this error:
```
17/06/12 01:48:03 ERROR server.TransportRequestHandler: Error sending result StreamResponse{streamId=/jars/importer-all.jar, byteCount=194727686, body=FileSegmentManagedBuffer{file=/tmp/importer-all.jar, offset=0, length=194727686}} to /10.211.XX.XX:39149; closing connection
java.nio.channels.ClosedChannelException
    at io.netty.channel.AbstractChannel$AbstractUnsafe.close(...)(Unknown Source)
17/06/12 01:48:03 WARN scheduler.TaskSetManager: Lost task 6.0 in stage 0.0 (TID 6, ip-10-211-127-63.ap-northeast-2.compute.internal, executor 9): java.nio.channels.ClosedChannelException
    at org.apache.spark.network.client.StreamInterceptor.channelInactive(StreamInterceptor.java:60)
    at org.apache.spark.network.util.TransportFrameDecoder.channelInactive(TransportFrameDecoder.java:179)
    at io.netty.channel.AbstractChannelHandlerContext.invokeChannelInactive(AbstractChannelHandlerContext.java:251)
    at io.netty.channel.AbstractChannelHandlerContext.invokeChannelInactive(AbstractChannelHandlerContext.java:237)
    at io.netty.channel.AbstractChannelHandlerContext.fireChannelInactive(AbstractChannelHandlerContext.java:230)
    at io.netty.channel.DefaultChannelPipeline$HeadContext.channelInactive(DefaultChannelPipeline.java:1289)
    at io.netty.channel.AbstractChannelHandlerContext.invokeChannelInactive(AbstractChannelHandlerContext.java:251)
    at io.netty.channel.AbstractChannelHandlerContext.invokeChannelInactive(AbstractChannelHandlerContext.java:237)
    at io.netty.channel.DefaultChannelPipeline.fireChannelInactive(DefaultChannelPipeline.java:893)
    at io.netty.channel.AbstractChannel$AbstractUnsafe$7.run(AbstractChannel.java:691)
    at io.netty.util.concurrent.SingleThreadEventExecutor.runAllTasks(SingleThreadEventExecutor.java:408)
    at io.netty.channel.nio.NioEventLoop.run(NioEventLoop.java:455)
    at io.netty.util.concurrent.SingleThreadEventExecutor$2.run(SingleThreadEventExecutor.java:140)
    at io.netty.util.concurrent.DefaultThreadFactory$DefaultRunnableDecorator.run(DefaultThreadFactory.java:144)
    at java.lang.Thread.run(Thread.java:745)

17/06/12 01:48:03 INFO scheduler.TaskSetManager: Starting task 6.1 in stage 0.0 (TID 541, ip-10-211-126-250.ap-northeast-2.compute.internal, executor 72, partition 6, PROCESS_LOCAL, 6680 bytes)
17/06/12 01:48:03 ERROR server.TransportRequestHandler: Error sending result StreamResponse{streamId=/jars/importer-all.jar, byteCount=194727686, body=FileSegmentManagedBuffer{file=/tmp/importer-all.jar, offset=0, length=194727686}} to /10.211.XX.XX:39151; closing connection
java.nio.channels.ClosedChannelException
    at io.netty.channel.AbstractChannel$AbstractUnsafe.close(...)(Unknown Source)
```
It's very likely due to your data is skewed, you'll want to properly pre-split your HBase table into appropriate region servers.

2. You might need to customize your own Partitioner, I use this one to partition the RDDs to match the region in HBase, so that when we load
the HFiles, each HFile will just be loaded onto that region, no HFile split is going to happen. (Splitting HFiles could be extremely time-consumingm, this
is why estimate your data size, pre-split your HBase table and do good partitioning is super important if you care about speed.) 


Reference: 
http://www.opencore.com/blog/2016/10/efficient-bulk-load-of-hbase-using-spark/

http://blog.cloudera.com/blog/2015/06/how-to-scan-salted-apache-hbase-tables-with-region-specific-key-ranges-in-mapreduce/