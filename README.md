# tableau_opt
make tableau work better

## TableauHyper

使用hyper-sql中的COPY命令直接从CSV导入数据至hyper文件，速度非常快，经过测试600万行数据31列将近17秒可以转换完成。  

### buildTableDefination

利用pandas.DataFrame生成对应的hyper表结构

### csv2hyper

使用hyper表结构把对应的csv文件转成Tableau的数据源hyper文件  

### 优点与缺点  

转换本身效率非常高，但是必须经过csv文件，从数据库查询的结果也这样处理有点绕了，数据写出csv还是挺慢的，如果是现成的csv文件，又必须读取为对应的pandas.DataFrame以供生成表结构。本人在使用中，试图pandas.read_csv时只读前1000行来节省csv读写的时间，有些数据前1000行不能代表整体来让pandas猜测数据类型，如果已知数据类型，我们完全可以搞个空的DataFrame了，但是那样就手动而不自动了。后来找到R中data.table在python中对应的datatable进行csv读写，600万行31列数据读写都是10几秒完成，datatable可以抽样转pandas用于生成hyper表结构，当然不在乎转换的15秒也是可以不抽样的。  

### 后续可能的优化

梳理datatable的数据类型与hyper的对应关系，从datatable直接生成hyper表结构，绕过pandas的使用。  

### 借鉴  

- pandas与tableau hyper的数据类型对应关系，及生成表结构，借用[innobi/pantab](https://github.com/innobi/pantab)中的代码，节省了大量时间。  
- 从CSV到hyper的转化，来自[Tableau hyper-api-samples](https://github.com/tableau/hyper-api-samples)打包成函数以便复用方便。  
- [chinchon/python-tableau-tde](https://github.com/chinchon/python-tableau-tde)给了最初的启发，使用它进行了第一次代码创建tableau数据源的尝试，但是迫于自己python水平弱，不会多线程写TDE文件，速度无法满足千万级的数据，没有继续。  
- [jlmorton/tableau](https://github.com/jlmorton/tableau)是之前一直使用的版本，多线程创建TDE，帮助非常巨大，但是java自己不会改，在python中调用jar命令，python又是crontab控制的shell脚本中执行的，每天总有一些jar进程，python写TDE的进程占着内存不退出，不清楚为什么。速度依然是不够快，既然使用python了，大量数据都在内存中，5G的数据加上计算过程中的数据等，python脚本能占10G，20G很正常，TDE文件创建过程中大量的时间内存都被占着，导致服务器资源紧张。这也是寻找新方法的动力。  

### 使用环境  

CPU: E5 12 cores * 2  
SSD: Intel S4510   
  
个人电脑中未测试速度，以后补上。600万行31列数据，之前用的jlmorton/tableau开到12线程也要10mins，同样的服务器上，现在的hyper API直接用hyper-sql命令COPY CSV生成hyper文件只要17秒，而且不需要专门开多线程，超过30倍的速度提升。  
当然这个提升是COPY命令带来的，如果用hyper api中的insert方法，只能按行循环插入数据，1秒仅能8000行，而且是用panda.DataFrame.iterrows()，是比之前的tde快得多的，但是600万行也要12分钟以上。于是权衡之下，还是麻烦一步写出csv吧。至于数据增量，删减，全用其他方式完成，最后都是一个csv几十秒转hyper，毕竟其他的数据处理日常也是免不了的。    


### 测试脚本

可使用`Test/test_csv_to_hyper.py`进行测试使用，行数`sample_df_nrows`，列数为原有的3列再加上重复生成`sample_df_ncols`列。 
   
在个人电脑上100万行，30列csv转化耗时8秒多  
  
电脑硬件如下:  
```
MacBook Pro (Retina, 13-inch, Early 2015)  
Processor 2.7 GHz Dual-Core Intel Core i5  
Memory 8 GB 1867 MHz DDR3  
```
  

测试结果:  
```
Create df 2021-03-31 14:08:57.060926
(1000000, 30)
Write csv start 2021-03-31 14:08:59.215759
100% |██████████████████████████████████████████████████| Writing CSV [done]
Build hyper file start 2021-03-31 14:09:01.871565
The number of rows in table "RandomValues" is 1000000.
Ends: 2021-03-31 14:09:10.330701
create df costs: 0:00:02.154833
Write csv cost: 0:00:02.655806
hyper file Costs: 0:00:08.459136
```
