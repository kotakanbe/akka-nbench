
Akka-nbench 
========================================================

Overview
------------

Akka-nbench is a benchmarking tool created to conduct accurate and repeatable performance tests and stress tests, and produce performance graphs.

AKka-nbench focuses on two metrics of performance:

- Throughput: number of operations performed in a timeframe,
   captured in aggregate across all operation types
- Latency: time to complete single operations, captured in
 quantiles per-operation

Generated Graph as below.
![sample_graph](https://raw.githubusercontent.com/wiki/kotakanbe/akka-nbench/images/sqs_put_c100_0.5kb_cpu35_.png)



Installation
------------

Docker
------------

See TODO


Manual Installation
------------

You first need to have JDK and Typesafe Activator.

[Oracle JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)

[Typesafe Activator](https://typesafe.com/get-started)

You should install [R](http://www.r-project.org) for Graph Generation.

```shell
$ sudo apt-get install r-base
$ R
> install.packages(c("getopt", "ggplot2", "reshape", "plyr", "proto", "digest"))
> q()
```


Quick Start
-------------

```shell
$ git clone https://github.com/kotakanbe/akka-nbench.git
$ cd akka-nbench
$ activator -mem 512 "run-main bench.Bench sleep"
$ activator -mem 512 "run-main bench.Summarizer" 
$ make results
$ open tests/current/summary.png
```

Customizing your Benchmark Options
-------------

https://github.com/kotakanbe/akka-nbench/blob/master/src/main/resources/application.conf
```
sleep {                         # Scenario_name
  concurrent = 100              # Sec
  duration = 10                 # Sec
  driver = "bench.drivers.SleepDriver"  # FQCN of Driver Class
  operations = {
    "sleep1" = 6                # operation_name = ratio
    "sleep2" = 4                # operation_name = ratio

  }
}
```

If you defined as above, it means
-  by 100 Threads(Akka actor)
-  for 10 Seconds
-  The Driver Class is bench.drivers.SleepDriver
-  sleep1 ... 60 Threads ( 6 / (6 + 4) * 100 )
-  sleep2 ... 40 Threads ( 4 / (6 + 4) * 100 )

How to add your Custom Driver
-------------

```shell
$ cd akka-nbench/src/main/scala/drivers
$ cp SleepDriver.scala MyDriver.scala
$ vim
```

akka-nbench/src/main/scala/drivers/MyDriver.scala
- ClassName
```
class MyDriver(...)
```
- Method Contents
```
  def sleep3(): (Boolean, Long, Long) = {
    val start = System.currentTimeMillis
    log.info("sleep3")
    Thread.sleep(1000)
    val endAt = System.currentTimeMillis
    val elapsedMillis= endAt - start
    (true, endAt, elapsedMillis)
  }
```
- getOperataion method
```
  override val getOperation = () => {
    operation match {
      case "sleep3" => sleep3 _
    }
  }
```

akka-nbench/src/main/resources/application.conf

```
mydriver {
  concurrent = 10
  duration = 10
  driver = "bench.drivers.MyDriver"
  operations = {
    "sleep3" = 1
  }
}
```

Benchmarking
```shell
$ cd akka-nbench
$ activator -mem 512 "run-main bench.Bench mydriver"
$ activator -mem 512 "run-main bench.Summarizer"
$ make results
$ open tests/current/summary.png
```

Multi-Clients benchmark
-------------

Benchmaking on Multi Clients and Collect raw.csv.
Then cat and sort...

```shell
$ cat raw1.csv raw2.csv raw3.csv | sort > raw.csv
$ activator -mem 512 "run-main bench.Summarizer"
$ make results
$ open tests/current/summary.png
```



Supported Platforms
-------------------
Unix/Linux

FAQ
---

etc.
---

