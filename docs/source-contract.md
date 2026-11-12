# 配送扫描 OCF 子集

文件以 Obj 和字节 1 开始；所有整数使用 zigzag long。头部 metadata map 支持正/负块计数，负计数后跟准确的本块字节长度，键和值均为字节串；以零计数终止，再跟 16 字节 sync。metadata 含 avro.schema 和可选 avro.codec；codec 为 null 或 RFC1951 原始 deflate。数据块为记录数、压缩负载长度、负载、相同 sync。

模式支持 record、string、int、long、bytes、boolean、双臂 nullable union 和 array，数组的负计数块也要校验字节长度。writer/reader 字段按名解析，reader 新字段要有合法默认值，允许 int 提升为 long，其他不支持的演进在接收前拒绝；不含递归模式。解压块字节、单条记录字节、数组元素数分别配置上限。

业务 Scan 含 eventId，以 reader 规范记录判等；同键异值阻塞整块并留冲突。恢复绑定源身份、头部摘要和已提交前缀摘要，不能仅看路径、mtime 或大小。末块不足属于等待，完整块 sync 损坏是错误，不扫描下个 marker。scans.avro 有三块，growing.avro 缺最后五字节，header.avro 仅含头部；scripts.materialize 还原版本化二进制夹具。只有记录数、压缩负载、sync 全部核对后才提交业务记录与块偏移。
