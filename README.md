# StormHdfsDemo
Storm 写入到 Hdfs Demo
使用一个简单的生成随机字符串的 Spout ，使用 HdfsBolt 将数据写入到 Hdfs 中。
自定义文件转化策略，每写入10条数据，即发生文件转化。新文件名为发生转换次数。
