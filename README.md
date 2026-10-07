# pulsar-data

Pulsar 数据集成包：可插拔数据源适配器框架、行情与参考数据归一化、本地市场数据湖（Parquet）与全市场历史回填 CLI。

本仓是 Pulsar（独立 A 股量化交易系统）多仓布局中的数据仓，仅依赖
[`pulsar-contracts`](https://github.com/vanzeph/pulsar-contracts)（端口契约与领域对象），
不依赖 pulsar-core / pulsar-exec，也不包含任何策略或交易逻辑。

## 能力总览

- **SourceAdapter 内部接口**：每个数据源实现 `fetch_raw`（调上游 SDK 或 HTTP）→ `normalize`（源格式转 canonical schema），由框架统一做质量校验后写入数据湖。
- **akshare 适配器**（首期主源）：日线（原始价 + 复权因子）、公司行为（分红送转 / 配股）、交易日历、全市场标的清单、停牌记录。
- **baostock 适配器**（免费无凭据；日线备源 / **分钟级主源**）：日线（原始价 + 复权因子，`后复权/不复权` 同端点派生）、交易日历、**5/15/30/60 分钟线**（原始价 + 全历史复权因子事件 as-of 合并的累计因子）；匿名 `login/logout` 会话封装；复用出网校验、限流退避与连续失败熔断。
- **主备路由与降级**：`SourceRouter` 按配置顺序声明 sources，主源失败（限流 / 超时 / 断流）逐调用自动降级备源，连续失败熔断（冷却后半开探测）；降级事件结构化留痕于 `<lake>/_meta/degradation_events.jsonl`。
- **双源交叉校验**：`CrossValidator` 在双源覆盖重叠区间抽样比对收盘价 / 成交量，差异超阈值记质量事件并产出 JSON 差异报告。
- **本地数据湖**：按 `标的 × 年` 分区的 Parquet 存储（日线 `bars_1d` 与分钟族 `bars_5min / bars_15min / bars_30min / bars_60min` 同构分区），分区级原子覆盖写（补数幂等），`_meta/watermarks.parquet` 记录每源每分区同步水位。
- **分钟级数据**：baostock 分钟线入湖——上游按"区间结束时刻"标记的 bar 归一化为左闭区间起点；质量门校验时段网格（09:30/13:00 锚定、午休排除）；完整性按"每交易日满 48/16/8/4 根"判定，未解释缺根即 gap；实测上游分钟覆盖自 **2020-06** 起（更早年份无数据，属已解释缺席）。
- **DuckDB 查询层**：`LakeQuery` 以只读语义的内存连接 + 会话视图直查 Parquet，非 SELECT/WITH 语句一律拒绝；读取与写入并发安全（写入方同进程内按分区串行）；分钟族按 `freq` 路由，缺族时自动从更细粒度**按需降采样**（OHLC 首/高/低/末，量额求和，因子取末）。
- **MarketDataPort 读侧**：`LakeMarketDataPort` 实现 `list_instruments / fetch_bars / fetch_corporate_actions / calendar`（`fetch_bars` = DuckDB 查询 + 按需复权，支持 `1d` 与 `5m/15m/30m/60m`），区间内未解释缺 bar（分钟级含未满时段）直接报错而非返回部分数据；`subscribe` 属实时链路（后续任务交付），显式 `NotImplementedError`。
- **按需复权**：湖只存原始价 + 累计因子，前复权 / 后复权在查询时按 `AdjustMode`（raw / forward / backward）派生，锚点取查询区间首/末 bar，同区间结果可复现。
- **增量更新**：`pulsar-data update` 以水位驱动日终增量，合并去重写入（不截断已有分区），重跑幂等；与回填共用同一条采集管线。
- **数据质量**：canonical schema 校验、OHLC 不变式、去重、相对交易日历的完整性报告（区分 `ok / not_listed / coverage_end / suspended / gap`，`gap` 为未解释缺 bar）。
- **质量标记与缺口闭环**：每行 bar 带 `quality` 标记列（`ok / backfilled / suspect`，未知取值被质量门拒绝）；交叉校验差异事件可落地为行级 `suspect` 标记（幂等、分区原子覆盖写）；`gap` 缺口自动转化为按分区的补数任务清单，整区覆盖重取写入（重复执行逐位一致、断点续跑）；分区级质量报告 + 查询层 `quality` 参数读侧过滤。
- **回填 CLI**：`pulsar-data backfill` 支持全市场或指定标的的历史回填，支持 `--freq 5m/15m/30m/60m` 分钟族回填（按标的 × 日历年分块取数，`--all` 全市场入口可回落湖内 instruments 快照），支持 `--fixture-dir` 离线回放（CI 与无网环境）。
- **快照流积累守护**（`pulsar-data snapshots`）：守护进程复用 D5 实时订阅通路（新浪主源 + 东财备源、主备降级、出网守卫、序号 / 迟到 / 缺失标记语义原样消费）把实时快照长期积累进数据湖新增 `snapshots` 分区族（**标的 × 日**分区、每订阅周期恰好一行、原子合并写与水位）；策略全可配（标的域默认自选清单、全市场约 1GB/日 需显式确认、采样频率上限、原始保留期限、到期降采样归档如 3s→1m）；断线重启自动续水位并把停机区间记入缺口台账（绝不静默丢帧）；磁盘水位监控与告警阈值；保留 / 归档任务幂等可重入。
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

### 分钟级历史回填（baostock，5/15/30/60 分钟）

```bash
# 指定域：20 只样本自 2020 年（上游分钟覆盖起点）至今的 5 分钟线
pulsar-data backfill --source baostock --lake ./data/lake \
    --start 2020-01-01 --end 2026-10-05 --freq 5m \
    --symbols SH600519,SZ000001,... --report minute-report.json --fail-on-gaps

# 全市场：--all 无上游清单时可回落到日线回填落好的 instruments 快照
pulsar-data backfill --source baostock --lake ./data/lake \
    --start 2020-01-01 --end 2026-10-05 --freq 5m --all

# 完整性复核（每交易日须满 48/16/8/4 根，缺根即未解释 gap）
pulsar-data verify --lake ./data/lake --start 2020-06-01 --end 2026-09-30 \
    --symbols SH600519 --freq 5m
```

分钟回填按 `标的 × 日历年` 分块取数（单次响应有界、重试代价小、分区整写），水位与
`quality=backfilled` 标记沿用既有机制；日域参考数据（instruments / 停牌 / 公司行为）
不在分钟路径重复摄取，停牌记录仍用于解释缺失的分钟日。

### 日终增量更新（水位驱动，可重入）

```bash
pulsar-data update --source akshare --lake ./data/lake --end 2026-10-05
```

每个标的从自身 bars 水位续拉（含水位当日重叠，可修复半日数据），合并去重写入既有
分区；重复执行同一 `--end` 结果逐字节一致。全新数据湖需补 `--initial-start YYYY-MM-DD`
（或先跑一次 backfill）。调度（cron 或 pulsar-app）由外部驱动，本包只保证任务可重入。

### 缺口补数与质量报告

```bash
# 检测交易日历基准的未解释缺 bar 并自动补数（按分区整区覆盖，幂等、断点续跑）
pulsar-data repair --source akshare --lake ./data/lake \
    --start 2024-01-01 --end 2024-12-31 \
    --report repair-report.json --task-list tasks.json

# 分区级质量标记汇总（ok / backfilled / suspect 计数与日期清单）
pulsar-data quality --lake ./data/lake --report quality-report.json
```

`repair` 把完整性报告中的 `gap` 格子按 `标的 × 年` 分区归并为补数任务（停牌、上市前、
覆盖期末均为已解释缺席，不生成任务），每个任务重取该分区整个日历年并整区原子覆盖写入
（行标 `backfilled`）；已完成任务的 id 持久化在 `<lake>/_meta/backfill_tasks.json`，
重跑跳过已完成项、重试失败项（`--force` 强制全部重执行），重复执行同一任务结果逐位一致。

## 质量标记与缺口闭环（库用法）

交叉校验差异落地为行级 `suspect` 标记（接受 `CrossCheckReport`、事件对象或序列化
JSON 报告中的事件字典；只改事件点名的行，分区原子覆盖重写，重复执行幂等）：

```python
from pulsar_data.quality_marks import mark_suspect

report = CrossValidator(akshare_adapter, baostock_adapter).check(["SH600519"], start, end)
result = mark_suspect(lake, report)
result.partitions      # 被重写的分区，如 ["symbol=SH600519/year=2024"]
result.rows_marked     # 标记为 suspect 的行数
```

缺口检测与补数任务执行：

```python
from pulsar_data.gapfill import detect_backfill_tasks, BackfillTaskExecutor

tasks = detect_backfill_tasks(lake, start=date(2024, 1, 1), end=date(2024, 12, 31))
# [BackfillTask(symbol="SH600519", year=2024, missing_days=(...)), ...]

executor = BackfillTaskExecutor(adapter, lake)
repair = executor.execute(tasks)   # 整区覆盖写；重跑幂等、断点续跑
repair.done, repair.failed, repair.skipped
```

分区级质量报告与读侧过滤：

```python
from pulsar_data.quality_report import build_quality_report

summary = build_quality_report(lake)          # 每分区 ok/backfilled/suspect 计数 + 日期清单
summary.totals                                # {"ok": ..., "backfilled": ..., "suspect": ...}
summary.to_json("quality-report.json")

with LakeQuery("./data/lake") as query:
    suspects = query.bars(["SH600519"], quality="suspect")        # 只要被标记行
    trusted = query.bars(quality=("ok", "backfilled"))            # 排除 suspect
    everything = query.bars()                                     # 不过滤（默认行为）
```

`MarketDataPort.fetch_bars` 契约签名不变、默认不过滤（标记过滤是查询层的可选参数），
避免破坏既有读侧行为。

## 主备路由与双源交叉校验（库用法）

路由器本身实现 `SourceAdapter` 协议，可直接交给 `IncrementalRunner` / `BackfillRunner`；
主源故障时逐调用自动降级备源，完成当日增量并留下降级事件：

```python
from pulsar_data.router import SourceRouter, DegradationLog

router = SourceRouter.from_config(
    [
        {"id": "akshare", "min_interval": 0.6},
        {"id": "baostock"},
    ],
    failure_threshold=3,          # 连续失败 3 次熔断该源
    cooldown=300.0,               # 熔断冷却（半开探测恢复）
    event_log=DegradationLog.for_lake("./data/lake"),
)
report = IncrementalRunner(router, lake).run(date(2026, 10, 5))
# 降级事件见 ./data/lake/_meta/degradation_events.jsonl
```

双源一致性抽样（重叠区间比对收盘价 / 成交量，差异超阈值记质量事件）：

```python
from pulsar_data.crosscheck import CrossValidator

report = CrossValidator(akshare_adapter, baostock_adapter,
                        close_tolerance=0.001, volume_tolerance=0.05,
                        sample_size=20).check(["SH600519"], start, end)
report.to_json("crosscheck-report.json")   # quality_events 列出全部超阈差异
```

## 读侧使用（查询层与端口）

研究者探索可直接用 DuckDB 查询层（只读），生产逻辑一律走 `MarketDataPort`：

```python
from datetime import date
from pulsar_contracts import AdjustMode, Freq
from pulsar_data import LakeMarketDataPort, LakeQuery

port = LakeMarketDataPort("./data/lake")

# 1) 端口读侧：fetch_bars = DuckDB 查询 + 按需复权（日线与分钟同签名）
bars = port.fetch_bars(["SH600519"], date(2024, 1, 1), date(2024, 12, 31),
                       Freq.DAILY, AdjustMode.FORWARD)
five = port.fetch_bars(["SH600519"], date(2024, 6, 3), date(2024, 6, 7),
                       Freq.MINUTE_5, AdjustMode.RAW)
instruments = port.list_instruments(date(2024, 6, 30))
actions = port.fetch_corporate_actions("SH600519")
trade_days = port.calendar(date(2024, 1, 1), date(2024, 12, 31))

# 2) DuckDB 直查（探索分析）：视图按需注册，只接受 SELECT/WITH
with LakeQuery("./data/lake") as query:
    frame = query.bars(["SH600519"], date(2024, 1, 1), date(2024, 12, 31))
    minutes = query.bars(["SH600519"], freq=Freq.MINUTE_15)   # 缺族时自动从 5m 降采样
    custom = query.query("SELECT symbol, count(*) AS n FROM bars_1d GROUP BY symbol")
```

读侧行为约定：请求区间内出现未解释缺 bar（既非上市前/覆盖期末，也非停牌；分钟级还要求
每个在覆盖内的交易日满时段——48/16/8/4 根）时抛 `DataNotAvailable`，绝不静默返回部分
数据；`Freq.MINUTE`（1m）暂无数据源，请求即报配置错误；实时订阅属实时链路任务，
当前显式 `NotImplementedError`。

## 数据湖布局

```text
lake/
  bars_1d/symbol=SH600519/year=2024/part.parquet   # 日线，按标的+年分区
  bars_5min/symbol=SH600519/year=2024/part.parquet  # 分钟族与日线同构分区
  bars_15min/symbol=SH600519/year=2024/part.parquet
  bars_30min/symbol=SH600519/year=2024/part.parquet
  bars_60min/symbol=SH600519/year=2024/part.parquet
  corporate_actions/symbol=SH600519/part.parquet
  instruments/instruments.parquet
  calendar/calendar.parquet
  suspensions/symbol=SH600519/part.parquet
  snapshots/symbol=SH600519/date=2026-10-05/part.parquet   # 实时快照，按标的+日分区
  snapshots_1m/symbol=SH600519/date=2026-09-01/part.parquet # 快照到期降采样归档（如 3s→1m）
  _meta/watermarks.parquet                          # 每源每分区更新水位
  _meta/snapshot_state.json                         # 快照守护持久水位（按标的 last seq/ts）
  _meta/snapshot_gaps.jsonl                         # 快照停机缺口台账（断线重连记缺口）
  _meta/snapshot_alerts.jsonl                       # 磁盘水位告警记录
  _meta/snapshot_policy.json                        # 守护运行时策略快照（status/archive 复用）
  _meta/backfill_tasks.json                         # 补数任务断点状态（已完成任务 id）
```

- `bars_1d` 与分钟族 canonical 列一致：`symbol, ts, open, high, low, close, volume, amount, adjust_factor, quality`。
- 只存原始价格与复权因子；前复权 / 后复权在查询时按 `AdjustMode` 派生（日线复权因子由
  上游后复权价 / 原始价逐日推导；分钟族复权因子由 `query_adjust_factor` 全历史事件
  as-of 合并——实测与日线口径逐位一致）。
- 时间戳统一 Asia/Shanghai；日线 `ts` 为该交易日 `00:00`，分钟 bar `ts` 为区间左端点
  （上游按区间**末**端标记，归一化时平移到左闭约定；首根 5 分钟 bar 为 `09:30`）。

## 分钟级磁盘量级估算（实测外推）

以真实录制数据经完整 `normalize → 质量门 → Parquet` 管线落盘实测（20 标的 × 跨年窗口，
`tests/fixtures/baostock/manifest.json` 的 `disk_measure`）：

| 实测项 | 数值 |
|-|-|
| 完整分区字节密度（816 行以上分区） | 33–50 B/row（中位 ≈ 42 B/row） |
| 单标的单年 5 分钟行数（48 根 × 242 交易日） | 11,616 行 ≈ 0.38–0.58 MB |
| 全市场单年（≈ 5,400 标的） | ≈ 2.1–3.2 GB/年（中位 ≈ 2.6 GB） |
| `bars_5min` 全量（上游覆盖 2020-06 → 2026-09，约 6.3 年） | **≈ 13–20 GB**（中位 ≈ 17 GB） |
| 加齐 15/30/60 分钟族（行数 ≈ 1/3、1/6、1/12） | 合计 ≈ 21–32 GB |

预算结论：消费级笔记本磁盘可承载全市场四档分钟全历史；若只落 `bars_5min`
（其余三档查询时按需降采样），全量约一二十 GB。
- `quality` 取值 `ok / backfilled / suspect`（未知取值在质量门被拒）：常规增量与参考数据写入
  `ok`，历史回填与缺口补数写入 `backfilled`，交叉校验差异行落地 `suspect`；读侧经
  `LakeQuery.bars(quality=...)` 按标记过滤。

## 复权口径

数据湖保存原始价与累计复权因子 `adjust_factor`（由源端后复权收盘 / 原始收盘逐日推导）。
查询时按 `AdjustMode` 派生（`pulsar_data.adjust.derive_adjusted`）：

- 后复权（BACKWARD）：锚点 = 查询区间内该标的**第一根** bar 的因子，
  后复权价 = 原始价 × `adjust_factor[t]` / `adjust_factor[区间首日]`。
- 前复权（FORWARD）：锚点 = 查询区间内该标的**最后一根** bar 的因子，
  前复权价 = 原始价 × `adjust_factor[t]` / `adjust_factor[区间末日]`（最新一日保持原始价）。
- RAW：原样返回。

锚点取自查询区间内部，因此同一 (标的, 区间) 的复权结果只取决于区间内数据本身，
不随湖内其余历史增减而变化（回测可复现）；两种模式都严格保持任意两日的总收益比率。
`volume / amount` 不做缩放（股本变动已体现在因子里），`adjust_factor` 列原样透出以便反推。
因子随上游分红送转自动回溯修订时，无需重写历史分区即可复算任意基准日的前复权序列。

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

## 实时快照订阅（Paper / Live）

`pulsar_data.realtime` 提供尽力而为（best-effort）的实时快照通路：采集器轮询公开免费行情接口，
订阅分发器按 `MarketDataPort.subscribe` 契约把快照推给消费者。

- **采集源**（`pulsar_data.realtime.collector`）：新浪 `hq.sinajs.cn` 批量报价为主源
  （五档盘口 + 源端报价时钟），东方财富 `push2` 批量报价为备源（最新价 / 量额，无五档）；
  `FailoverQuoteSource` 按声明顺序路由、记录降级事件；两者复用 D1 的出网安全校验
  （`SafeHTTPSession`）、每源限速（`RateLimiter`）与熔断（`CircuitBreaker`）。
- **订阅分发**（`pulsar_data.realtime.dispatcher`）：`subscribe(symbols, on_snapshot) -> Subscription`
  返回契约的 `Subscription`（`unsubscribe()` 幂等）；每个快照带**按标的单调递增的 `seq`**
  与 Asia/Shanghai 时间戳。**迟到与缺失从不抛异常**：缺失周期消耗其 `seq`（缺口即可见标记），
  并通过 `subscribe_events` 发出显式 `MISSING / LATE` 标记事件（`StreamEvent`），消费者回调抛
  异常只计数不外传。
- **端口衔接**（`pulsar_data.realtime.mixin`）：`RealtimeSubscriptionMixin` 以组合方式把
  `subscribe` 混入读侧 `MarketDataPort` 实现（D3 交付于 `pulsar_data.port.LakeMarketDataPort`，
  其 `subscribe` 按分工保留 `NotImplementedError`）：

```python
from pulsar_data.port import LakeMarketDataPort                # D3 读侧
from pulsar_data.realtime import RealtimeSubscriptionMixin

class MarketDataService(RealtimeSubscriptionMixin, LakeMarketDataPort):
    pass

service = MarketDataService("./data/lake")
subscription = service.subscribe(["SH600519", "SZ000001"], on_snapshot=lambda s: print(s.seq, s.last_price))
...
subscription.unsubscribe()

# 需要显式观察迟到 / 缺失标记时：
subscription = service.subscribe_events(["SH600519"], on_event=lambda e: print(e.kind, e.symbol, e.seq, e.reason))
```

测试全部离线（解析用真实端点形状的 canned 载荷）；`tests/network/` 下有一个可选的
`@network` 实网冒烟，默认跳过，显式开启：

```bash
PULSAR_RUN_NETWORK_TESTS=1 pytest tests/network -m network
```

## 快照流积累守护（本地先行）

`pulsar_data.snapshots` 把上面的实时通路变成长期数据资产：一个守护进程订阅
D5 通道（只消费、不改其语义），把**每个订阅周期的每个标的**持续写入数据湖
`snapshots` 分区族（标的 × 日分区），并配套保留 / 降采样归档、缺口台账与磁盘
水位监控。初期全部落本地磁盘，零云端依赖。

### 启停与状态

```bash
# 前台跑一把（自选清单，Ctrl-C / SIGTERM 优雅退出并落最后一次刷盘）
pulsar-data snapshots collect --lake ./data/lake --symbols SH600519,SZ000001

# 守护方式：detached 启动（pidfile 于 <lake>/_meta/collect.pid，日志于 _meta/collect.log）
pulsar-data snapshots start --lake ./data/lake --config ./snapshot-policy.json
pulsar-data snapshots status --lake ./data/lake    # 运行状态 + 水位 + 缺口 + 磁盘报告
pulsar-data snapshots stop  --lake ./data/lake

# 保留 / 降采样归档（幂等，可交给 crontab 每日跑一次）
pulsar-data snapshots archive --lake ./data/lake
```

pulsar-app 或 systemd 直接托管 `snapshots collect` 前台进程即可（SIGTERM 优雅停机）；
`start/stop` 是轻量的 detached 封装，适合手工运维。

### 策略配置（JSON）

```json
{
  "symbols": ["SH600519", "SZ000001"],
  "full_market": false,
  "acknowledge_full_market": false,
  "poll_interval_s": 3,
  "min_sample_interval_s": 0,
  "flush_interval_s": 5,
  "raw_retention_days": 14,
  "archive_interval_s": 60,
  "reconnect_gap_threshold_s": 10,
  "disk_warn_free_percent": 10,
  "disk_warn_used_bytes": null
}
```

- **标的域**：默认自选清单（`symbols`）。全市场（`"full_market": true`，标的取湖内
  instruments 快照）约 **1GB/日**，必须同时显式 `"acknowledge_full_market": true`
  （CLI `--all --accept-full-market`），否则加载即报配置错误——磁盘预算绝不静默默认。
- **采样频率上限**（`min_sample_interval_s`）：每标的至多每 N 秒落一条行情；被抽稀的
  周期落 `kind=thinned` 标记行（占位不占数据），缺口语义不受影响。
- **保留与归档**：`raw_retention_days` 天前的原始分区聚合为 `snapshots_1m`
  （`archive_interval_s`，默认 3s→1m：OHLC 取报价 `last_price` 首/高/低/末、量额取
  累计值跨度、附 samples/missing/thinned 计数），归档落盘后才删除原始目录；
  `archive_retention_days`（默认 0 = 永久）再控制归档本身的清理。
- **断线与缺口**：守护重启时读取 `_meta/snapshot_state.json` 的持久水位续采；同一交易日
  内超过 `reconnect_gap_threshold_s` 的停机写入 `_meta/snapshot_gaps.jsonl` 台账
  （免费源无快照回填能力，区间如实记录而非谎称完整）。周期级缺失以 `kind=missing`
  行落盘、迟到以 `kind=late` 落盘——**每个周期恰好一行，绝不静默丢帧**。
- **磁盘水位**：每次刷盘检查快照族字节数与所在卷剩余空间，越过阈值写
  `_meta/snapshot_alerts.jsonl` 并在 `status` 标红。

### 磁盘预算（估算）

单条快照行约 25 列（五档簿展开）：全市场约 5,400 标的 × 4 小时 / 3s ≈ 2,600 万行/日，
约 **0.8–1.2 GB/日**（与设计预算 ~1GB/日 一致）；20 标的自选清单约 **4–5 MB/日**。
归档后 1 分钟行数仅为原始的约 1/20（3s 档），长期保留成本可忽略。默认策略
（自选 + 14 天原始保留）总占用约 `标的数 × 70 MB` 量级；全市场 14 天原始保留约
**14–17 GB**，请按 `disk_warn_used_bytes` 设告警并配短保留期或低采样档。

### 库用法

```python
from pulsar_data.lake import DataLake
from pulsar_data.snapshots import SnapshotCollectorDaemon, SnapshotPolicy, archive_expired

lake = DataLake("./data/lake")
policy = SnapshotPolicy.from_mapping({"symbols": ["SH600519"], "raw_retention_days": 14})
daemon = SnapshotCollectorDaemon(lake, policy)   # 真实源：新浪主源 + 东财备源
daemon.start()
...
daemon.stop()                                    # 优雅停机：末次刷盘 + 水位落盘

archive_expired(lake, policy)                    # 保留/降采样归档任务（幂等）
```

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
多源部署时 `SourceRouter` 在适配器熔断之外再做跨源故障切换：主源失败逐调用降级
备源、连续失败整源跳过（冷却半开探测），每次降级写入 `_meta/degradation_events.jsonl`
（from/to 源、数据集、标的、原因、连续失败数、是否熔断），湖内写入与水位始终归属
实际供数源。

## 测试

```bash
pip install -e ".[dev]"
pytest
```

- 全部测试离线运行：实时源响应以 fixture 形式录制于 `tests/fixtures/akshare/`
  （录制脚本 `scripts/record_fixtures.py`，manifest 记录来源与版本）；baostock 以
  Mock SDK（login/logout + ResultData 游标协议的假模块）与录制假客户端驱动，
  CI 不依赖外网。
- 分钟级验收（fixture 驱动，`tests/fixtures/baostock/` 由 `scripts/record_minute_fixtures.py`
  录制真实上游响应）：20 标的样本域 × 跨年窗口（2020-06 起，上游实测分钟覆盖起点）
  5 分钟回填**零未解释缺口**；`fetch_bars` 各分钟档与湖内数据逐行一致；日线全量回归保持绿。
- 出网安全、归一化、质量校验、湖写入原子性（并发读写竞争）/ 水位 / 增量幂等重入、
  复权核对（构造分红送转样本的手工算例 + 录制茅台真实分红样本与源端口径交叉验证）、
  20 标的 × 1 年完整回填均有独立测试。
- 主备路由：主源故障注入 → 自动降级 baostock → 完成当日增量的全流程演练、
  熔断 / 冷却半开 / 降级事件留痕、双源交叉校验（一致通过 / 超阈差异报告）均有独立测试。
- 快照流积累：模拟快照流端到端（守护写入 → 重启续采 → 水位补齐 → 停机缺口台账 +
  周期级 missing/late 标记落盘、每周期恰好一行）、采样抽稀（`thinned` 占位不破坏
  `seq` 连续语义）、刷盘失败缓冲保留（绝不静默丢帧）、保留 / 降采样归档（到期聚合、
  原始回收、重跑幂等、聚合失败保原始）、全市场开关守卫（未显式确认即拒绝）、
  CLI 启停 / 状态 / 归档 / 真实子进程 SIGTERM 停机均有离线测试；
  `tests/network/` 另有可选实网冒烟（默认跳过）。

## License

MIT
