package main

import (
    "bufio"
    "context"
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"

    "github.com/twmb/franz-go/pkg/kgo"
    "github.com/twmb/franz-go/pkg/sasl/plain"
    "github.com/twmb/franz-go/pkg/sasl/scram"
)

var clientVersion = "dev"

type Spec struct {
    RunID string `json:"runId"`
    Kind string `json:"kind"`
    Version string `json:"version"`
    Bootstrap string `json:"bootstrap"`
    Topic string `json:"topic"`
    SecurityProtocol string `json:"securityProtocol"`
    SASLMechanism string `json:"saslMechanism"`
    SASLUsername string `json:"saslUsername"`
    SASLPassword string `json:"saslPassword"`
    CAPath string `json:"caPath"`
    Key string `json:"key"`
    Value string `json:"value"`
    Count int `json:"count"`
    Rate float64 `json:"rate"`
    DurationSeconds int `json:"durationSeconds"`
    ConnectionTimeoutMs int `json:"connectionTimeoutMs"`
    RequestTimeoutMs int `json:"requestTimeoutMs"`
    MaxMessageBytes int `json:"maxMessageBytes"`
    Retries int `json:"retries"`
    MaxFetchBytes int `json:"maxFetchBytes"`
    FetchMaxWaitMs int `json:"fetchMaxWaitMs"`
    ClientConfig map[string]any `json:"clientConfig"`
    LogLevel string `json:"logLevel"`
    GroupID string `json:"groupId"`
    OffsetStart string `json:"offsetStart"`
    Commit bool `json:"commit"`
    Partition *int32 `json:"partition"`
}

func emit(event, message string, details map[string]any) {
    out := map[string]any{"event": event, "message": message}
    for key, value := range details { out[key] = value }
    encoded, _ := json.Marshal(out)
    fmt.Println(string(encoded))
}

func intOption(spec Spec, key string, fallback int) int {
    if value, ok := spec.ClientConfig[key].(float64); ok { return int(value) }
    return fallback
}

func options(spec Spec) ([]kgo.Opt, error) {
    brokers := strings.Split(spec.Bootstrap, ",")
    for i := range brokers { brokers[i] = strings.TrimSpace(brokers[i]) }
    opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ClientID("kcl-" + spec.Kind + "-" + spec.RunID),
        kgo.DialTimeout(time.Duration(intOption(spec, "dialTimeoutMs", spec.ConnectionTimeoutMs)) * time.Millisecond),
        kgo.RequestTimeoutOverhead(time.Duration(intOption(spec, "requestTimeoutMs", spec.RequestTimeoutMs)) * time.Millisecond),
        kgo.MetadataMaxAge(time.Duration(intOption(spec, "metadataMaxAgeMs", 300000)) * time.Millisecond)}
    level := kgo.LogLevelInfo
    if spec.LogLevel == "debug" { level = kgo.LogLevelDebug }
    opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, level, nil)))
    if spec.SecurityProtocol == "SSL" || spec.SecurityProtocol == "SASL_SSL" {
        tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
        if spec.CAPath != "" {
            pem, err := os.ReadFile(spec.CAPath)
            if err != nil { return nil, err }
            pool, err := x509.SystemCertPool()
            if err != nil { pool = x509.NewCertPool() }
            if !pool.AppendCertsFromPEM(pem) { return nil, errors.New("CA 证书不是有效 PEM") }
            tlsConfig.RootCAs = pool
        }
        opts = append(opts, kgo.DialTLSConfig(tlsConfig))
    }
    if strings.HasPrefix(spec.SecurityProtocol, "SASL_") {
        switch spec.SASLMechanism {
        case "PLAIN":
            opts = append(opts, kgo.SASL(plain.Auth{User: spec.SASLUsername, Pass: spec.SASLPassword}.AsMechanism()))
        case "SCRAM-SHA-256":
            opts = append(opts, kgo.SASL(scram.Auth{User: spec.SASLUsername, Pass: spec.SASLPassword}.AsSha256Mechanism()))
        case "SCRAM-SHA-512":
            opts = append(opts, kgo.SASL(scram.Auth{User: spec.SASLUsername, Pass: spec.SASLPassword}.AsSha512Mechanism()))
        }
    }
    return opts, nil
}

func simulate(ctx context.Context, spec Spec) error {
    opts, err := options(spec)
    if err != nil { return err }
    if spec.Kind == "producer" && spec.Partition != nil {
        opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
    }
    if spec.Kind == "producer" {
        opts = append(opts, kgo.ProduceRequestTimeout(time.Duration(intOption(spec, "produceRequestTimeoutMs", spec.RequestTimeoutMs)) * time.Millisecond),
            kgo.RecordDeliveryTimeout(time.Duration(intOption(spec, "deliveryTimeoutMs", spec.RequestTimeoutMs + 5000)) * time.Millisecond),
            kgo.RecordRetries(intOption(spec, "recordRetries", spec.Retries + 1)),
            kgo.ProducerBatchMaxBytes(int32(intOption(spec, "batchMaxBytes", spec.MaxMessageBytes + 1024))),
            kgo.ProducerLinger(time.Duration(intOption(spec, "lingerMs", 0)) * time.Millisecond))
    }
    if spec.Kind == "consumer" {
        opts = append(opts, kgo.FetchMaxBytes(int32(intOption(spec, "fetchMaxBytes", spec.MaxFetchBytes))),
            kgo.FetchMaxPartitionBytes(int32(intOption(spec, "fetchMaxPartitionBytes", 1048576))),
            kgo.FetchMaxWait(time.Duration(intOption(spec, "fetchMaxWaitMs", spec.FetchMaxWaitMs)) * time.Millisecond),
            kgo.FetchMinBytes(int32(intOption(spec, "fetchMinBytes", 1))))
        offset := kgo.NewOffset().AtEnd()
        if spec.OffsetStart == "earliest" { offset = kgo.NewOffset().AtStart() }
        if spec.Partition != nil {
            opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
                spec.Topic: {*spec.Partition: offset},
            }))
        } else {
            group := spec.GroupID
            if group == "" { group = "kcl-" + spec.RunID }
            opts = append(opts, kgo.ConsumerGroup(group), kgo.ConsumeTopics(spec.Topic), kgo.ConsumeStartOffset(offset))
            if !spec.Commit { opts = append(opts, kgo.DisableAutoCommit()) }
        }
    }
    cl, err := kgo.NewClient(opts...)
    if err != nil { return err }
    defer cl.Close()
    pingCtx, cancel := context.WithTimeout(ctx, time.Duration(intOption(spec, "requestTimeoutMs", spec.RequestTimeoutMs))*time.Millisecond)
    err = cl.Ping(pingCtx)
    cancel()
    if err != nil { return err }
    emit("ready", "已连接 Broker", nil)
    deadline := time.Now().Add(time.Duration(spec.DurationSeconds) * time.Second)
    completed := 0
    if spec.Kind == "producer" {
        start := time.Now()
        for completed < spec.Count && time.Now().Before(deadline) && ctx.Err() == nil {
            value := strings.ReplaceAll(strings.ReplaceAll(spec.Value, "{seq}", fmt.Sprint(completed+1)), "{runId}", spec.RunID)
            payloadBytes := len([]byte(spec.Key)) + len([]byte(value))
            maxMessageBytes := intOption(spec, "sim.max.message.bytes", spec.MaxMessageBytes)
            if payloadBytes > maxMessageBytes {
                return fmt.Errorf("消息 Key 与内容共 %d 字节，超过设定上限 %d 字节", payloadBytes, maxMessageBytes)
            }
            record := &kgo.Record{Topic: spec.Topic, Key: []byte(spec.Key), Value: []byte(value)}
            if spec.Partition != nil { record.Partition = *spec.Partition }
            sentAt := time.Now()
            result, err := cl.ProduceSync(ctx, record).First()
            if err != nil { return err }
            completed++
            emit("sent", "消息已确认", map[string]any{"topic": result.Topic, "partition": result.Partition,
                "offset": result.Offset, "latencyMs": time.Since(sentAt).Milliseconds()})
            target := start.Add(time.Duration(float64(completed) / spec.Rate * float64(time.Second)))
            if pause := time.Until(target); pause > 0 {
                select { case <-ctx.Done(): break; case <-time.After(pause): }
            }
        }
        emit("done", "生产任务完成", map[string]any{"count": completed})
        return nil
    }
    for completed < spec.Count && time.Now().Before(deadline) && ctx.Err() == nil {
        pollCtx, cancel := context.WithTimeout(ctx, time.Second)
        fetches := cl.PollFetches(pollCtx)
        cancel()
        fetches.EachError(func(topic string, partition int32, err error) {
            if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
                emit("error", err.Error(), map[string]any{"topic": topic, "partition": partition})
            }
        })
        fetches.EachRecord(func(record *kgo.Record) {
            if completed >= spec.Count { return }
            completed++
            emit("received", "收到消息", map[string]any{"topic": record.Topic, "partition": record.Partition, "offset": record.Offset})
        })
    }
    emit("done", "消费任务完成", map[string]any{"count": completed})
    return nil
}

func main() {
    reader := bufio.NewScanner(os.Stdin)
    reader.Buffer(make([]byte, 4096), 4_194_304)
    if !reader.Scan() { emit("error", "未收到运行配置", nil); os.Exit(1) }
    var spec Spec
    if err := json.Unmarshal(reader.Bytes(), &spec); err != nil { emit("error", err.Error(), nil); os.Exit(1) }
    if spec.Version != clientVersion {
        emit("error", "客户端版本不匹配", nil); os.Exit(1)
    }
    fmt.Fprintf(os.Stderr, "[runner] PID %d · franz-go %s · %s starting\n", os.Getpid(), clientVersion, spec.Kind)
    emit("client", "franz-go "+clientVersion+" 已加载", nil)
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()
    if err := simulate(ctx, spec); err != nil && !errors.Is(err, context.Canceled) {
        emit("error", err.Error(), nil); os.Exit(1)
    }
}
