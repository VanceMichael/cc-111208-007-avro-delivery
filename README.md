# deliveryocf

配送扫描 Avro OCF 文件头诊断与逐块接收工具。

## 本地运行

使用 Python 3.11 标准库，无 pip 依赖。所有来源与业务数据都是本地合成数据，不连接真实用户系统。

```sh
python -m scripts.materialize
python -m deliveryocf header fixtures/scans.avro
python -m compileall -q .
python -m unittest discover -s tests -v
python -m scripts.demo
```

`header` 命令输出单份来源的结构摘要，保持独立诊断用途。`capture URL --method GET --state var` 仅接受 loopback HTTP，保存原始响应为 SHA256 文件，并在 SQLite captures 表登记；可用 `--header 'Name:Value'` 传头、`--body FILE` 传正文，不跟随重定向。

`ingest FILE --state DIR --reader READER.json` 是新增的逐块接收：按 docs/source-contract.md 的子集核对每个完整块的记录数、压缩负载与尾部 sync 后，在单个 SQLite 事务里同时提交业务记录与块偏移检查点；尾块不足时等待，源被替换或已提交前缀被修改时隔离（退出码 3），不沿用旧偏移。限额 `--max-block-bytes`（解压后）、`--max-record-bytes`、`--max-array-elements` 分别配置。同 eventId 同规范内容去重，同 id 异值阻塞整块并留存冲突。`scans --state DIR` 把已接收扫描、阻塞块、原文偏移、writer schema 指纹与来源身份关联输出。接收流程的验收边界见 docs/acceptance.md。

## 可复现容器

```sh
docker build -t deliveryocf:local .
docker run --rm --network none deliveryocf:local python -m unittest discover -s tests -v
docker run --rm --network none deliveryocf:local
```

镜像固定基础摘要。构建在正式运行前完成；运行和测试不安装包、不下载数据。演示命令先执行原有检查器，再用本地合成文件演示接收闭环：前两块到齐、末块缺尾时等待，补齐后第二次运行只新增第三块，全程不超过 90 秒。

## 协议与来源

- docs/source-contract.md 给出支持子集、字段及失败语义。
- docs/acceptance.md 给出业务验收边界。
- fixtures/source.json 是本地可重放 HTTP 路由；`python -m scripts.source_server --port 8765` 启动，`/__requests` 可检查实际请求。
- scripts/materialize.py 将版本化 base64 二进制夹具还原为原始字节，重复执行安全；二进制文件不是用户附件。
- journal.py 只记录原始响应，不代替新增流程自己的事务和恢复设计。
