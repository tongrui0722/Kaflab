"use strict";
const fs = require("fs");
const path = require("path");
const readline = require("readline");
let stopping = false;
process.on("SIGTERM", () => { stopping = true; });
process.on("SIGINT", () => { stopping = true; });
const emit = (event, message, extra = {}) => process.stdout.write(JSON.stringify({ event, message, ...extra }) + "\n");
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const option = (spec, key, fallback) => spec.clientConfig?.[key] ?? fallback;

function connection(spec, kafka) {
  const cfg = { clientId: `kcl-${spec.kind}-${spec.runId}`, brokers: spec.bootstrap.split(",").map(s => s.trim()).filter(Boolean),
    connectionTimeout: option(spec, "connectionTimeout", spec.connectionTimeoutMs),
    requestTimeout: option(spec, "requestTimeout", spec.requestTimeoutMs),
    maxInFlightRequests: option(spec, "maxInFlightRequests", 5),
    retry: { retries: option(spec, "retry.retries", spec.retries), initialRetryTime: 250 },
    logLevel: spec.logLevel === "debug" ? kafka.logLevel.DEBUG : kafka.logLevel.INFO };
  if (spec.securityProtocol === "SSL" || spec.securityProtocol === "SASL_SSL") {
    cfg.ssl = spec.caPath ? { ca: [fs.readFileSync(spec.caPath, "utf8")] } : true;
  }
  if (spec.securityProtocol.startsWith("SASL_")) {
    const mechanism = spec.saslMechanism === "PLAIN" ? "plain" : spec.saslMechanism.toLowerCase();
    cfg.sasl = { mechanism, username: spec.saslUsername, password: spec.saslPassword };
  }
  return new kafka.Kafka(cfg);
}

async function produce(spec, kafka) {
  const partitioner = option(spec, "partitioner", spec.version.startsWith("2.") ? "default" : "legacy");
  const partitioners = kafka.Partitioners;
  const createPartitioner = spec.version.startsWith("2.")
    ? (partitioner === "legacy" ? partitioners.LegacyPartitioner : partitioners.DefaultPartitioner)
    : (partitioner === "java-compatible" ? partitioners.JavaCompatiblePartitioner : partitioners.DefaultPartitioner);
  const producer = connection(spec, kafka).producer({ allowAutoTopicCreation: false, createPartitioner,
    metadataMaxAge: option(spec, "metadataMaxAge", 300000) });
  await producer.connect();
  emit("ready", "已连接 Broker");
  const start = Date.now();
  try {
    for (let i = 0; i < spec.count && !stopping && Date.now() - start < spec.durationSeconds * 1000; i++) {
      const value = spec.value.replaceAll("{seq}", String(i + 1)).replaceAll("{runId}", spec.runId);
      const payloadBytes = Buffer.byteLength(spec.key || "", "utf8") + Buffer.byteLength(value, "utf8");
      const maxMessageBytes = option(spec, "sim.max.message.bytes", spec.maxMessageBytes);
      if (payloadBytes > maxMessageBytes) throw new Error(`消息 Key 与内容共 ${payloadBytes} 字节，超过设定上限 ${maxMessageBytes} 字节`);
      const message = { key: spec.key || null, value };
      if (spec.partition !== null) message.partition = spec.partition;
      const sentAt = Date.now();
      const response = await producer.send({ topic: spec.topic, messages: [message],
        acks: option(spec, "acks", "all") === "all" ? -1 : 1,
        compression: option(spec, "compression", "none") === "gzip" ? kafka.CompressionTypes.GZIP : kafka.CompressionTypes.None });
      const first = response[0] || {};
      emit("sent", "消息已确认", { topic: spec.topic, partition: first.partition, offset: first.baseOffset,
        latencyMs: Date.now() - sentAt });
      await sleep(Math.max(0, start + (i + 1) * 1000 / spec.rate - Date.now()));
    }
    emit("done", "生产任务完成");
  } finally { await producer.disconnect(); }
}

async function consume(spec, kafka) {
  if (spec.partition !== null) throw new Error("KafkaJS 档位暂不支持指定分区直读，请使用消费组订阅");
  const consumer = connection(spec, kafka).consumer({ groupId: spec.groupId || `kcl-${spec.runId}`,
    metadataMaxAge: option(spec, "metadataMaxAge", 300000),
    maxBytes: option(spec, "maxBytes", spec.maxFetchBytes),
    maxBytesPerPartition: option(spec, "maxBytesPerPartition", 1048576),
    maxWaitTimeInMs: option(spec, "maxWaitTimeInMs", spec.fetchMaxWaitMs),
    minBytes: option(spec, "minBytes", 1),
    sessionTimeout: option(spec, "sessionTimeout", 30000),
    heartbeatInterval: option(spec, "heartbeatInterval", 3000) });
  await consumer.connect();
  let count = 0;
  let finish;
  const finished = new Promise(resolve => { finish = resolve; });
  try {
    await consumer.subscribe({ topic: spec.topic, fromBeginning: spec.offsetStart === "earliest" });
    consumer.on(consumer.events.GROUP_JOIN, event => emit("assigned", "消费组已分配分区", { count: event.payload.memberAssignment ? Object.keys(event.payload.memberAssignment).length : 0 }));
    await consumer.run({ autoCommit: spec.commit, eachMessage: async ({ topic, partition, message }) => {
      if (stopping) return;
      count++;
      emit("received", "收到消息", { topic, partition, offset: message.offset });
      if (count >= spec.count) finish();
    }});
    emit("ready", "消费者已启动");
    const timer = setInterval(() => { if (stopping) finish(); }, 100);
    await Promise.race([finished, sleep(spec.durationSeconds * 1000)]);
    clearInterval(timer);
    emit("done", `消费任务完成，共读取 ${count} 条`, { count });
  } finally { await consumer.disconnect(); }
}

async function main() {
  const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  const spec = JSON.parse(await new Promise(resolve => input.once("line", resolve)));
  input.close();
  const root = process.env.KCL_KAFKAJS_ROOT;
  const packageRoot = path.join(root, "node_modules", "kafkajs");
  const installed = require(path.join(packageRoot, "package.json")).version;
  if (installed !== spec.version) throw new Error(`客户端版本不匹配：要求 ${spec.version}，实际 ${installed}`);
  const kafka = require(packageRoot);
  process.stderr.write(`[runner] PID ${process.pid} · KafkaJS ${installed} · ${spec.kind} starting\n`);
  emit("client", `KafkaJS ${installed} 已加载`);
  if (spec.kind === "producer") await produce(spec, kafka);
  else await consume(spec, kafka);
}
main().catch(error => { emit("error", `${error.name || "Error"}: ${error.message || error}`); process.exitCode = 1; });
