# AI 美股投研与交易辅助系统 (Lightweight Quant Research System)

本项目旨在构建一个基于 **AI 大模型 (LLM) + 结构化财务数据 + 传统量化指标** 的轻量级美股投研系统。

系统第一阶段不直接追求全自动实盘交易，而是先实现：

* 自动发现市场热点
* 将热点映射到相关美股标的
* 用结构化财务数据做硬筛选
* 用 AI 辅助阅读财报文本和新闻材料
* 输出每日候选股评分、观察清单、分析报告和交易计划

后续在回测和模拟盘稳定后，再考虑接入真实交易通道。实盘阶段默认采用“系统生成信号 + 人工确认下单”的模式，避免过早进入不可控的全自动交易。

---

## Polymarket 天气交易模块

本仓库新增了 `usstock.polymarket_weather` 子系统，用于研究 Polymarket 天气温度桶市场，重点支持每日最高温、最低温的区间桶交易。

模块定位是“预测 + Edge + 仓位建议 + 交易日志 + 回测”，默认不自动下单。真实交易执行应在长期模拟盘稳定、合规确认和人工确认流程完善后再接入。

### 功能范围

* 支持任意城市配置：上海、香港、东京、纽约、伦敦只是内置示例，任何城市都可以通过经纬度、时区、结算站和结算单位扩展。
* 支持多模型 Ensemble：Open-Meteo 数值预报模型，例如 `ecmwf_ifs025`、`gfs_seamless`、`ukmo_seamless`；美国城市可额外接入 `nws`。
* 支持 Polymarket Gamma API 市场发现和 outcome 价格解析，并可选从 CLOB midpoint 刷新价格。
* 自动解析常见温度桶 outcome，例如 `74 or below`、`75 to 76`、`90 or above`。
* 使用 ensemble 均值、模型分歧和误差下限构造正态分布，输出每个温度桶的概率。
* 计算 Edge：`我的概率 - 市场隐含概率`。
* Kelly Criterion 仓位控制，支持 Full / Half / Quarter Kelly。
* 内置单笔、单日、单市场、单城市风险敞口上限。
* 本地 JSON 缓存、运行日志、CSV 交易日志和历史信号回测。

### 目录结构

```text
src/usstock/polymarket_weather/
  buckets.py       # 温度桶解析与生成
  cache.py         # 本地 JSON TTL 缓存
  cli.py           # usstock weather 命令行入口
  config.py        # 城市、市场、API 配置
  engine.py        # 预测引擎，整合天气、市场、概率和 Edge
  market.py        # Polymarket Gamma / CLOB adapter
  probability.py   # 温度概率分布
  risk.py          # Kelly 仓位与风险上限
  ledger.py        # CSV 交易日志与绩效统计
  backtest.py      # 历史信号回测
  weather.py       # Open-Meteo / NWS 天气源
```

示例配置文件：

```text
config/polymarket_weather.example.yaml
```

### 安装

```bash
python -m pip install -e .
```

依赖以免费、开源库为主。当前核心逻辑使用 Python 标准库；项目依赖包含 `pandas`、`pyyaml` 等。YAML 配置需要 `pyyaml`。

### 快速运行

控制面板入口：

```bash
usstock admin
```

打开本地控制面板后，左侧导航进入 **天气交易**。如果服务早已启动，需要重启 `usstock admin` 进程才能看到新入口。

查看某城市某天 ensemble 预报：

```bash
usstock weather forecast --city new-york --date 2026-07-03 --kind high
```

查看 Polymarket 市场桶：

```bash
usstock weather market \
  --city new-york \
  --query "New York high temperature July 3"
```

生成 Edge + Kelly 仓位建议：

```bash
usstock weather signal \
  --city new-york \
  --date 2026-07-03 \
  --kind high \
  --query "New York high temperature July 3" \
  --bankroll 1000 \
  --kelly half \
  --min-edge 0.03
```

手动计算一个 YES 桶仓位：

```bash
usstock weather size \
  --outcome "85 to 86" \
  --probability 0.38 \
  --price 0.29 \
  --bankroll 1000 \
  --kelly quarter
```

记录 signal 输出里的最佳建议到本地交易日志：

```bash
usstock weather signal \
  --city new-york \
  --date 2026-07-03 \
  --kind high \
  --query "New York high temperature July 3" \
  --record-best
```

查看本地绩效统计：

```bash
usstock weather performance
```

### 城市与结算站扩展

每个城市建议显式配置：

* `latitude` / `longitude`：天气 API 查询点。
* `timezone`：目标城市时区。
* `settlement_station`：Polymarket 结算规则指定的气象站或官方来源。
* `settlement_unit`：`F` 或 `C`。
* `weather_models`：模型列表，例如 `ecmwf_ifs025`、`gfs_seamless`、`ukmo_seamless`、`nws`。
* `model_weights`：对特定模型加权，例如短临时提高 ECMWF 权重。
* `model_error_std`：模型误差下限，避免 ensemble 过度自信。

示例：

```yaml
cities:
  shanghai:
    name: Shanghai
    latitude: 31.2304
    longitude: 121.4737
    timezone: Asia/Shanghai
    settlement_station: "Configure to the exact Polymarket resolution source"
    settlement_unit: C
    weather_models:
      - ecmwf_ifs025
      - gfs_seamless
      - ukmo_seamless
    model_weights:
      ecmwf_ifs025: 1.2
    model_error_std: 1.4
    min_distribution_std: 0.8
```

运行时指定配置：

```bash
usstock weather forecast \
  --config config/polymarket_weather.example.yaml \
  --city shanghai \
  --date 2026-07-03 \
  --kind high
```

### 回测

历史回测使用 CSV 输入。最小字段：

```csv
city_id,target_date,kind,outcome,probability,market_price,settled_outcome,market_slug,token_id
new-york,2026-07-01,high,80,0.60,0.50,true,nyc-high-temp,
new-york,2026-07-02,high,81,0.60,0.50,false,nyc-high-temp,
```

运行：

```bash
usstock weather backtest \
  --csv data/weather_backtest_sample.csv \
  --bankroll 1000 \
  --kelly half \
  --min-edge 0.03
```

输出包含交易数、胜率、总盈亏、ROI、最大回撤和期末资金。

### 风控逻辑

YES 合约以 `price` 买入，若结算为 YES 支付 1。Full Kelly 投入资金比例为：

```text
full_kelly_fraction = (probability - price) / (1 - price)
```

系统随后应用：

* `kelly_mode`：`full`、`half`、`quarter`。
* `min_edge`：低于阈值不交易。
* `max_trade_fraction`：单笔最大资金占比。
* `max_daily_fraction`：单日最大新增敞口。
* `max_market_fraction`：单市场最大敞口。
* `max_city_fraction`：单城市最大敞口。

### 数据源说明

* Open-Meteo Forecast API：用于免费天气模型预报。
* NWS API：用于美国城市的 National Weather Service 预报补充。
* Polymarket Gamma API：用于发现市场、读取问题、outcomes、outcome prices 和 CLOB token ids。
* Polymarket CLOB API：可选用于刷新 midpoint。

相关官方文档：

* Open-Meteo Forecast API: https://open-meteo.com/en/docs
* Polymarket Gamma Markets API: https://docs.polymarket.com/developers/gamma-markets-api/get-markets
* Polymarket CLOB Price API: https://docs.polymarket.com/developers/CLOB/prices-books/get-price

### 测试

```bash
python -m unittest \
  tests/test_polymarket_weather_buckets.py \
  tests/test_polymarket_weather_probability.py \
  tests/test_polymarket_weather_market.py \
  tests/test_polymarket_weather_risk.py \
  tests/test_polymarket_weather_backtest.py
```

这些测试不访问网络，覆盖核心交易逻辑。真实 API 调用建议单独做集成测试，并控制频率与缓存。

---

## 可实现性结论

整体思路 **可以实现**，但需要调整原始设想中的几个高风险点：

* 新闻、社交媒体和行情数据源需要分层使用，不能假设都有免费、稳定、无限量 API。
* `yfinance` 适合原型、研究和部分回测，但不应作为严肃实盘的唯一数据源。
* 财报关键数字必须由 Python 从 SEC、行情商或财务数据接口读取，不能让大模型直接“读数做判断”。
* AI 更适合做文本摘要、风险提取、主题归类和业务相关性判断，不适合单独负责交易决策。
* 自动下单必须放在最后阶段，并加入模拟盘、人工确认、仓位限制、止损、熔断和审计日志。
* 币安或代币化美股通道存在地区、合规、流动性和产品可用性限制，不能作为默认交易通道。

因此，本项目更合理的定位是：

> 先做一个可验证的 AI 投研与信号生成系统，再逐步演进到半自动交易，而不是一开始就做全自动交易机器人。

---

## 系统核心架构

### 模块一：热点自动发现 (Trend Detection)

**目标：** 自动捕获华尔街、财经媒体里的新增热点，减少人工刷新闻的时间。

#### 1.1 数据源接入

MVP 第一批只接 3 类核心数据源，先保证覆盖硬公告、全球新闻主题和金融新闻，不在第一版里铺太多重复源。

**第一批数据源：**

* SEC EDGAR：必接。它不是传统新闻源，但对美股热点发现非常关键，重点关注 8-K、10-Q、10-K、S-1、重大公告、财报披露和风险提示。SEC 的 `data.sec.gov` API 不需要认证，适合做“硬事件源”。官方文档：https://www.sec.gov/search-filings/edgar-application-programming-interfaces
* GDELT DOC API：必接。用于发现宏观、行业、政策、地缘、AI、能源、医药、半导体等主题热度，适合统计新闻数量、来源覆盖、关键词增长率和跨区域传播。文档入口：https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
* Finnhub News：建议第一版接。它提供 market news 和 company news，能按市场分类或 ticker 获取财经新闻，落地成本比直接抓财经网站低，可作为金融新闻主源。官方文档：https://finnhub.io/docs/api/market-news

**暂缓接入的数据源：**

* Yahoo Finance 新闻：可以作为原型期 RSS 或页面补充，但不要作为核心依赖。公开接口稳定性和使用边界都不适合作为严肃数据管道的唯一来源。
* NewsAPI：暂不作为第一批。它适合开发测试和补充新闻源，但免费额度、延迟和生产使用限制会影响持续轮询。
* Alpha Vantage News & Sentiment：暂不作为第一批。接口方便，但免费额度较紧，更适合后续低频补充或验证情绪标签。
* Polygon/Massive News：质量和覆盖不错，但更偏付费增强源。Phase 2 或愿意直接采购数据时再接，届时可用于替换或增强 Finnhub。
* Google News：可以用于 RSS 或搜索结果辅助，但不应假设存在稳定免费的官方 API。
* X/Twitter：官方 API 成本和限制较高，适合后续阶段再接入。
* 社区讨论源：暂缓接入。MVP 不使用社区信号参与热点发现和候选股评分。

**第一版落库建议：**

* 所有原始文章和公告先保存原始标题、摘要、正文片段、发布时间、来源 URL、来源名称、抓取时间和原始 payload。
* 统一抽取 `source_type`，例如 `sec_filing`、`global_news`、`financial_news`。
* 对每条记录提取候选 ticker、关键词、主题标签和语言信息。
* 去重时优先使用 URL、标题规范化哈希、发布时间窗口和正文相似度。
* 热点评分时将 SEC 事件权重设高，金融新闻和全球新闻负责提供持续传播证据。

#### 1.2 文本清洗与主题提取

Python 负责：

* 去重
* 去广告和低质量内容
* 提取标题、摘要、发布时间、来源、相关 ticker
* 统计关键词频率和变化率

AI 负责：

* 归并相似主题
* 提取新增叙事
* 过滤纯噪音
* 为主题生成简短解释

#### 1.3 热度打分

每个主题计算一个 `trend_score`，可包含：

* 新闻数量
* 来源权重
* 过去 24 小时或 7 天增长率
* 是否出现 SEC 公告、财报、重大订单或政策事件

每日输出前 3-5 个热点主题。

---

### 模块二：相关公司筛选 (Company Screening)

**目标：** 将模糊热点映射到具体美股标的，剔除纯蹭概念的公司。

#### 2.1 股票池初始化

股票池建议分阶段建设：

* MVP 阶段：先使用 Nasdaq 100、S&P 500、热门中概股和手工 watchlist。
* 扩展阶段：接入 Nasdaq/NYSE/AMEX 全市场 ticker 列表。
* 稳定阶段：补充行业分类、市值、成交额、做空比例、期权活跃度等字段。

#### 2.2 关键词与向量匹配

候选匹配可以分两层：

* 传统匹配：公司名称、业务描述、行业、产品关键词。
* 向量匹配：将公司业务描述和热点主题 embedding 后做相似度检索。

#### 2.3 AI 业务纯度审计

AI 只做业务相关性判断，不直接给买卖建议。

输入：

* 热点主题
* 公司业务描述
* 最新新闻摘要
* 财报中相关业务段落

输出：

* `relevance_score`：0-100
* `pure_play_level`：核心受益者 / 部分相关 / 边缘相关 / 蹭热点
* 简短理由
* 需要人工复核的疑点

#### 2.4 候选股票池

每个热点最终保留 3-5 只候选股，并记录：

* ticker
* 公司名称
* 所属主题
* 相关性分数
* 流动性
* 市值区间
* 最新触发新闻或事件

---

### 模块三：财报与基本面审计 (Fundamental Auditing)

**目标：** 验证热点公司是否有真实业绩支撑，避免只靠叙事选股。

#### 3.1 结构化财务数据硬筛选

Python 负责读取和计算关键数字，数据源优先级：

* SEC EDGAR company facts / submissions API
* yfinance
* Financial Modeling Prep、Polygon、Tiingo、Alpha Vantage 等第三方接口

核心规则示例：

* 营收同比增速 > 20%
* 毛利率没有明显恶化
* 自由现金流或净利润趋势健康
* 现金和负债结构没有明显恶化
* 最近 3 个月没有异常稀释、退市风险或重大诉讼警示

注意：

* 大模型不得直接读取关键财务数字并作为最终依据。
* 所有进入评分系统的数字必须保留来源、时间戳和原始字段。

#### 3.2 财报文本分析

AI 负责分析 10-Q、10-K、8-K 和 earnings call transcript 中的文本信息。

重点提取：

* 管理层业绩指引
* 需求变化
* 供应链风险
* 客户集中度
* 法律诉讼
* 库存积压
* 毛利率压力
* 是否存在与热点主题直接相关的收入或订单

#### 3.3 综合评分

输出 `fundamental_score`，建议由以下部分组成：

* 增长质量
* 盈利能力
* 现金流质量
* 资产负债风险
* 管理层表述
* 热点主题兑现程度

最终系统只输出“候选”和“观察”，不直接输出“必须买入”。

---

### 模块四：技术面择时与交易计划 (Technical Timing)

**目标：** 避免在情绪高潮追高，寻找更合理的入场区间。

#### 4.1 技术指标

MVP 阶段建议本地计算，不依赖 TradingView 私有接口：

* RSI
* MACD
* 20/50/200 日均线
* 成交量变化
* ATR 波动率
* 相对大盘强弱

可使用：

* pandas
* pandas-ta
* ta
* yfinance 或其他行情接口

#### 4.2 买入观察条件

示例条件：

* 基本面评分达标
* 主题热度仍在上升或未明显衰退
* 股价回踩 20 日或 50 日均线附近
* RSI 从过热区回落后企稳
* 成交量没有出现异常放大下跌

系统输出的是：

* 观察价位
* 入场条件
* 无效条件
* 止损参考位
* 仓位建议

#### 4.3 风险控制

默认规则：

* 单只股票最大仓位不超过总资金的 10%-25%
* 单日新增风险敞口设上限
* 单笔交易必须有止损或退出条件
* 连续亏损触发暂停交易
* 所有信号、价格、评分和人工确认记录都写入日志

---

### 模块五：交易执行 (Execution)

**目标：** 在系统长期稳定后，再接入真实交易。

#### 5.1 推荐演进顺序

1. 只生成投研报告，不下单。
2. 接入 paper trading，验证信号和风控。
3. 小金额实盘，但每笔交易需要人工确认。
4. 只有在长期稳定后，才考虑部分自动化执行。

#### 5.2 可选交易通道

可调研：

* Interactive Brokers
* Alpaca
* Schwab
* 富途、老虎等券商 API
* 币安或其他代币化股票通道

注意：

* 交易通道受地区、账户资格、产品可用性和监管限制影响。
* 币安代币化股票或股票交易服务不能假设所有地区都可用。
* 如果交易的是代币化资产，还需要额外考虑流动性、滑点、跟踪误差、交易时间和合约风险。

---

## MVP 开发路线

### 数据库迁移

项目使用 PostgreSQL。迁移代码会按 `migrations/*.sql` 文件名顺序执行 `-- migrate:up` 段，并自动创建 `schema_migrations` 表记录已经执行过的迁移。

```bash
.venv/bin/python -m usstock.db.migrations migrate
.venv/bin/python -m usstock.db.migrations status
```

如果项目已安装为可执行命令，也可以使用：

```bash
usstock migrate
usstock status
```

数据库连接默认读取环境变量或 `.env` 中的 `DATABASE_URL`。上服务器时直接让 `DATABASE_URL` 指向服务器已有 PostgreSQL，并确保目标数据库已经创建、连接用户具备建表和迁移权限。

### SEC EDGAR 接入

SEC EDGAR 不需要 API key，但必须配置带联系信息的 `SEC_USER_AGENT`。

```bash
.venv/bin/python -m usstock.data.sec sync-registry
.venv/bin/python -m usstock.data.sec sync-ticker AAPL --filing-limit 20
.venv/bin/python -m usstock.data.sec sync-ticker AAPL --include-company-facts --filing-limit 20 --fact-limit 500
```

如果项目已安装为可执行命令，也可以使用：

```bash
usstock sec sync-registry
usstock sec sync-ticker AAPL --filing-limit 20
usstock sec sync-ticker AAPL --include-company-facts --filing-limit 20 --fact-limit 500
```

### GDELT DOC API 接入

GDELT DOC API 不需要 API key。MVP 先使用 `artlist` 同步全球新闻文章列表，并用 `timelinevolraw` 保存主题热度原始时间线。

```bash
.venv/bin/python -m usstock.data.gdelt sync-articles '"artificial intelligence" semiconductor' --timespan 24h --max-records 50
.venv/bin/python -m usstock.data.gdelt sync-timeline '"artificial intelligence" semiconductor' --timespan 24h
.venv/bin/python -m usstock.data.gdelt sync-query '"artificial intelligence" semiconductor' --timespan 24h --max-records 50
```

如果项目已安装为可执行命令，也可以使用：

```bash
usstock gdelt sync-query '"artificial intelligence" semiconductor' --timespan 24h --max-records 50
```

### Finnhub News 接入

Finnhub News 需要配置 `FINNHUB_API_KEY`。MVP 支持 market news 分类新闻和 company news 个股新闻，原始响应会落到 `finnhub_news_queries`，标准化文章会落到 `finnhub_articles`。

```bash
.venv/bin/python -m usstock.data.finnhub sync-market --category general
.venv/bin/python -m usstock.data.finnhub sync-company AAPL --from-date 2026-06-01 --to-date 2026-06-09
```

如果项目已安装为可执行命令，也可以使用：

```bash
usstock finnhub sync-market --category general
usstock finnhub sync-company AAPL --from-date 2026-06-01 --to-date 2026-06-09
```

### 自动热点发现和每日候选股

自动发现流程会写入默认主题库，用主题库替代人工 GDELT query；同步 Finnhub market news 并抽取相关 ticker 和关键词；同步 GDELT 主题新闻；扫描股票池中的 SEC filings；最后生成每日候选股评分和观察清单。

```bash
usstock migrate
usstock discover seed-topics
usstock discover daily --top-n 25
```

从已入库的 Finnhub 和 GDELT 新闻中抽取候选主题：

```bash
usstock discover extract-topics --lookback-hours 72 --max-candidates 25
```

候选主题会先写入 `market_topic_candidates`，默认不会直接污染正式主题库。确认后可以按 slug 晋升到 `market_topics`：

```bash
usstock discover promote-topics --slug quantum_computing
```

如果只想用库内已有的 GDELT、Finnhub、SEC 数据重新评分，不触发外部请求：

```bash
usstock discover daily --skip-sync --top-n 25
```

基于已有候选评分和观察清单生成新闻驱动分析报告：

```bash
usstock report daily --top-n 10
usstock report daily --top-n 10 --save-markdown
```

报告会写入 `daily_analysis_reports`，同时可以保存 Markdown 到 `reports/`。默认使用规则模型生成事件摘要、相关理由、关注点和风险提示；如果需要 LLM 增强摘要和表达，可以在 `.env` 中配置 `REPORT_LLM_API_KEY`、`REPORT_LLM_BASE_URL`、`REPORT_LLM_MODEL` 和 `REPORT_LLM_REQUEST_TIMEOUT_SECONDS`，然后运行：

```bash
usstock report daily --top-n 10 --use-llm --save-markdown
```

### 日报复盘和回测

日报生成后，可以把后续日线价格同步到 `market_daily_prices`，再计算日报候选股在 T+1、T+5、T+20 的实际表现。第一版默认使用“日报日之后第一个有价格的交易日收盘价”作为入场参考价，避免使用日报当天之后才知道的信息。

默认使用 `yfinance` 同步 Yahoo Finance 日线数据，适合原型、研究和复盘场景：

```bash
usstock market sync-yfinance --ticker AAPL --ticker NVDA --from-date 2026-06-01 --to-date 2026-06-22
usstock market sync-yfinance --from-report-candidates --from-date 2026-06-01 --to-date 2026-06-22 --top-n 10
```

如果 Yahoo/yfinance 临时限流或不可用，也可以导入本地 CSV 日线价格：

```bash
usstock market import-prices data/raw/prices.csv
usstock market import-prices data/raw/NVDA.csv --ticker NVDA --data-source manual_csv
```

CSV 支持常见列名：`ticker`/`symbol`、`date`、`open`、`high`、`low`、`close`、`adj close`、`volume`、`currency`。如果没有 `ticker` 或 `symbol` 列，可以用 `--ticker` 指定。

复盘一段时间内已经持久化的日报：

```bash
usstock backtest reports --from-date 2026-06-01 --to-date 2026-06-22 --top-n 10
usstock backtest reports --from-date 2026-06-01 --to-date 2026-06-22 --price-source yfinance
```

复盘结果会写入 `daily_candidate_performance`，并在命令行输出整体胜率、平均收益、中位收益、平均回撤，以及按排名、评分区间和主题聚合后的 T+5 表现。

本地管理面板也提供同样入口：左侧进入“复盘”，或在“同步”页面切换到“日报复盘”标签，可以自动同步免费日线价格、运行日报复盘并查看最近复盘记录；CSV 导入作为备用入口保留。

如果需要让它按固定间隔循环执行，可以使用：

```bash
usstock discover loop --interval-minutes 60
```

默认配置会同步 Finnhub `general` 和 `merger` 两个 market news 分类；GDELT 会按主题库逐个同步；SEC 会扫描股票池中排序靠前的活跃标的。可以用 `--max-sec-tickers`、`--skip-gdelt-sync`、`--skip-finnhub-sync`、`--skip-sec-sync` 控制同步范围。

### 本地管理面板

项目提供一个自用的轻量本地面板，不依赖前端构建工具，默认只监听 `127.0.0.1`。

```bash
PYTHONPATH=src .venv/bin/python -m usstock.admin.app
```

如果默认端口被占用，可以显式指定端口：

```bash
PYTHONPATH=src .venv/bin/python -m usstock.admin.app --port 7879
```

如果项目已安装为可执行命令，也可以使用：

```bash
usstock admin
usstock admin --port 7879
```

默认访问地址：

```text
http://127.0.0.1:7878
```

面板仍然不需要登录。启动时会额外打印一个 `管理员动作地址`，用这个地址访问后，浏览器会写入仅当前面板可用的 HttpOnly Cookie；页面也会把当前 URL 里的 `admin_token` 自动带到站内链接和 POST 表单里。

普通地址可以免登录浏览，但同步、生成报告、维护股票池、主题审核和复盘写入等数据动作会进入只读状态。

如果需要让管理员动作地址在重启后保持不变，可以在 `.env` 中配置固定令牌：

```bash
ADMIN_ACTION_TOKEN=换成一段足够长的随机字符串
```

也可以启动时传入：

```bash
usstock admin --admin-action-token 换成一段足够长的随机字符串
```

第一版面板只覆盖最常用的内部操作：

* 查看股票池、SEC 公告、GDELT 文章、Finnhub 新闻、分析报告和最近迁移记录。
* 手工新增或更新观察标的。
* 触发 SEC 公司映射、单只 ticker、GDELT query 和 Finnhub News 同步。

### Phase 1：投研数据管道和日报

目标周期：1-2 周

实现内容：

* 建立项目结构
* 抓取新闻和基础行情
* 维护初始股票池
* 计算热点主题
* 对候选股票做基础财务硬筛选
* 生成 Markdown 或 HTML 每日投研报告

输出示例：

* 今日热点主题 Top 5
* 每个主题对应候选股
* 候选股财务硬指标
* AI 生成的业务相关性解释
* 需要人工复核的问题

### Phase 2：评分系统和回测

目标周期：3-5 周

实现内容：

* 建立统一评分模型
* 保存历史每日快照
* 回测过去 6-12 个月的信号表现
* 统计收益率、最大回撤、胜率、盈亏比和持仓周期
* 调整评分权重

这一阶段不接实盘。

### Phase 3：模拟盘和人工确认交易

目标周期：长期

实现内容：

* 接入 paper trading
* 模拟订单、止损、仓位和滑点
* 对每个交易信号生成解释
* 增加人工确认流程
* 记录每次确认、拒绝和修改

### Phase 4：小金额实盘

目标周期：长期，且必须在模拟盘稳定后开始

实现内容：

* 极小金额实盘
* 严格仓位限制
* API 权限最小化
* 错误熔断
* 每日审计报告

---

## 建议的初始目录结构

```text
usstock/
  readme.md
  pyproject.toml
  .env.example
  src/
    usstock/
      config/
      data/
        news.py
        market.py
        sec.py
      trends/
        extract.py
        score.py
      screening/
        universe.py
        match.py
        auditor.py
      fundamentals/
        metrics.py
        filings.py
        scoring.py
      technicals/
        indicators.py
        signals.py
      reports/
        daily_report.py
      backtest/
        engine.py
      execution/
        paper.py
        broker.py
  data/
    raw/
    processed/
    snapshots/
  migrations/
  reports/
  tests/
```

---

## 第一版验收标准

第一版不要求赚钱，也不要求自动交易。它只需要证明系统能稳定生成有用的投研结果。

验收标准：

* 可以一键生成当天热点报告。
* 每个推荐主题都有来源链接或原始文本依据。
* 每只候选股都有相关性解释和财务硬指标。
* 所有关键财务数字都有结构化来源。
* 系统能保存每日快照，方便后续回测。
* AI 输出只作为辅助解释，不覆盖结构化数据判断。

---

## 重要免责声明

本项目用于研究、学习和辅助投研，不构成投资建议。任何实盘交易都需要用户自行判断，并承担市场风险、流动性风险、接口风险和合规风险。
