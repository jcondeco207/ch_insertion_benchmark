# Clickhouse Insertion Benchmark

```
⚠️ Work in Progress ⚠️
```

## Objective
A benchmark on clickhouse bulk insertion for thousands of rows with JSONEachRow. (in different languages).
Languages to be tested:
- [ ] Rust
- [ ] Go
- [ ] Python
- [ ] Java
- [ ] Scala
- [ ] Swift

This way I'll be able to decide wich language (and sdk) is better for my thesis log parsing and insertion component.

## The tests

### Payload format

- id (uuid)
- devEui (String)
- device_name (String)
- metric_id (Int64)
- metric_name (String)
- metric_type (String)
- value (String)
- event_time (DateTime64)

### Single (test_id = 1)

Insert one by one 5000 and 10 000 logs into clickhouse.

### Bulk (test_id = 2)

Insert in bulk 5000 and 10 000 logs into clickhouse.

## Results

|Language| SDK | Payload Size | Time 1 | Time 2 | Time 3 |
|:------:|:---:|:------------:|:------:|:------:|:------:|
|Rust|clickhouse-rs|5000|  | | |
|Rust|clickhouse-rs|10000|  | | |
|Go|clickhouse-go|5000|  | | |
|Go|clickhouse-go|10000|  | | |
|Go|ch-go|5000|  | | |
|Go|ch-go|10000|  | | |
|Java|JDBC|5000|  | | |
|Java|JDBC|10000|  | | |
|Java|[Client](https://clickhouse.com/docs/integrations/language-clients/java/client)|5000| | | |
|Java|[Client](https://clickhouse.com/docs/integrations/language-clients/java/client)|10000| | | |
|Python|clickhouse-connect|5000| 0.051 | 0.040 | 0.047 |
|Python|clickhouse-connect|10000| 0.090 | 0.089 | 0.072 |


## Conclusions
