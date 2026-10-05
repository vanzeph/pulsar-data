# pulsar-data

Pulsar 数据集成包：可插拔数据源适配器框架、行情与参考数据归一化、本地市场数据湖（Parquet）与全市场历史回填 CLI。

本仓是 Pulsar（独立 A 股量化交易系统）多仓布局中的数据仓，仅依赖
[`pulsar-contracts`](https://github.com/vanzeph/pulsar-contracts)（端口契约与领域对象），
不依赖 pulsar-core / pulsar-exec，也不包含任何策略或交易逻辑。

## 能力总览

- **SourceAdapter 内部接口**：每个数据源实现 `fetch_raw`（调上游 SDK 或 HTTP）→ `normalize`（源格式转 canonical schema），由框架统一做质量校验后写入数据湖。
- **akshare 适配器**（首期主源）：日线（原始价 + 复权因子）、公司行为（分红送转 / 配股）、交易日历、全市场标的清单、停牌记录。
- **本地数据湖**：按 `标的 × 年` 分区的 Parquet 存储，分区级原子覆盖写（补数幂等），`_meta/watermarks.parquet` 记录每源每分区同步水位。
- **数据质量**：canonical schema 校验、OHLC 不变式、去重、相对交易日历的完整性报告（区分 `ok / not_listed / coverage_end / suspended / gap`，`gap` 为未解释缺 bar）。
- **回填 CLI**：`pulsar-data backfill` 支持全市场或指定标的的历史回填，支持 `--fixture-dir` 离线回放（CI 与无网环境）。
- **出网安全**：所有 HTTP 请求仅允许 http/https，发起前校验目标 host，拒绝 localhost / 环回 / 私有 / 保留地址；提供公共校验函数与全局 requests 守卫钩子。

## 安装

```bash
pip install git+https://github.com/vanzeph/pulsar-data.git          # 核心依赖
pip install "pulsar-data[akshare] @ git+https://github.com/vanzeph/pulsar-data.git"  # 含 akshare 实时源
```

开发安装：

```bash
git clone https://github.com/vanzeph/pulsar-data.git
cd pulsar-data
pip install -e ".[dev]"
pytest            # 全部离线，不访问网络
```

## 快速上手

### 全市场历史回填（需网络与 akshare）

```bash
pulsar-data backfill --source akshare --lake ./data/lake \
    --start 2024-01-01 --end 2024-12-31 --all
```

抽样回填 20 只标的并输出质量报告：

```bash
pulsar-data backfill --source akshare --lake ./data/lake \
    --start 2024-01-01 --end 2024-12-31 --all --limit 20 \
    --report backfill-report.json
```

### 离线回放（CI / 无网环境）

```bash
pulsar-data backfill --source akshare --lake ./data/lake \
    --start 2024-01-01 --end 2024-12-31 --all \
    --fixture-dir tests/fixtures/akshare --report backfill-report.json
```

`--fixture-dir` 读取 `scripts/record_fixtures.py` 录制的原始上游响应，适配器的归一化、
质量校验与入湖全链路照常执行，只是把“取数”换成“读本地录制”。测试套件即以此方式
在 CI 中实证“20 只标的 × 1 个完整年度回填、相对交易日历无未解释缺 bar”。

### 校验已有数据湖的完整性

```bash
pulsar-data verify --lake ./data/lake --start 2024-01-01 --end 2024-12-31
```

退出码非 0 表示存在未解释缺 bar。

## 数据湖布局

```text
lake/
  bars_1d/symbol=SH600519/year=2024/part.parquet   # 日线，按标的+年分区
  corporate_actions/symbol=SH600519/part.parquet
  instruments/instruments.parquet
  calendar/calendar.parquet
  suspensions/symbol=SH600519/part.parquet
  _meta/watermarks.parquet                          # 每源每分区更新水位
```

- `bars_1d` canonical 列：`symbol, ts, open, high, low, close, volume, amount, adjust_factor, quality`。
- 只存原始价格与复权因子；前复权 / 后复权在查询时按 `AdjustMode` 派生（复权因子由
  上游后复权价 / 原始价逐日推导，保证与源端复权口径一致）。
- 时间戳统一 Asia/Shanghai；日线 `ts` 为该交易日 `00:00`（左闭右开区间起点）。
- `quality` 取值 `ok / backfilled / suspect`；历史回填写入 `backfilled`。

## 复权口径

数据湖保存原始价与累计复权因子 `adjust_factor`（由源端后复权收盘 / 原始收盘逐日推导）。
查询时：

- 后复权价 = 原始价 × `adjust_factor`
- 前复权价 = 原始价 × `adjust_factor` / `adjust_factor[基准日]`

因此因子随上游分红送转自动回溯修订时，无需重写历史分区即可复算任意基准日的前复权序列。

## 出网安全

```python
from pulsar_data.netguard import validate_url, EgressViolation, install_egress_guard

validate_url("https://push2.eastmoney.com/api/qt/stock/kline/get")   # 通过
validate_url("http://192.168.1.1/")                                  # 抛 EgressViolation
validate_url("ftp://example.com/")                                   # 抛 EgressViolation（非 http/https）

with install_egress_guard():        # 守卫期间所有 requests 出网（含重定向跳转）均先校验
    ...
```

校验规则：仅 http/https；解析目标 host 的全部地址，拒绝环回（127/8、::1）、私有
（10/8、172.16/12、192.168/16、fc00::/7）、链路本地（169.254/16、fe80::/10）、
保留（0/8、100.64/10、192.0.0/24、192.0.2/24、198.18/15、240/4、255.255.255.255）、
组播（224/4、ff00::/8）及未指定地址；`localhost` 及其子域按名称直接拒绝。

## 适配器扩展

新增数据源 = 新增一个适配器 + 一条注册，核心框架零改动：

```python
from pulsar_data.sources import register_adapter, SourceAdapter, Dataset, FetchRequest

@register_adapter("my-source")
def build(config: dict | None = None) -> SourceAdapter:
    return MySourceAdapter(config)
```

框架按 `fetch_raw(dataset, request)` → `normalize(dataset, raw)` 的固定管线驱动所有源，
质量校验与入湖由框架统一执行，适配器只做采集与翻译。

## 限流与失败行为

适配器内置每源最小请求间隔与指数退避重试；连续失败熔断当次任务并逐标的记录，
单源故障不阻塞其他标的，缺口进入下一次补数（分区级原子覆盖写保证幂等）。

## 测试

```bash
pip install -e ".[dev]"
pytest
```

- 全部测试离线运行：实时源响应以 fixture 形式录制于 `tests/fixtures/akshare/`
  （录制脚本 `scripts/record_fixtures.py`，manifest 记录来源与版本）。
- 出网安全、归一化、质量校验、湖写入原子性 / 水位、复权因子一致性、
  20 标的 × 1 年完整回填均有独立测试。

## License

MIT
