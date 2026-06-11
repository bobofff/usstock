# AI 美股投研与交易辅助系统 (Lightweight Quant Research System)

本项目旨在构建一个基于 **AI 大模型 (LLM) + 结构化财务数据 + 传统量化指标** 的轻量级美股投研系统。

系统第一阶段不直接追求全自动实盘交易，而是先实现：

* 自动发现市场热点
* 将热点映射到相关美股标的
* 用结构化财务数据做硬筛选
* 用 AI 辅助阅读财报文本和新闻材料
* 输出每日候选股评分、观察清单和交易计划

后续在回测和模拟盘稳定后，再考虑接入真实交易通道。实盘阶段默认采用“系统生成信号 + 人工确认下单”的模式，避免过早进入不可控的全自动交易。

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

**目标：** 自动捕获华尔街、财经媒体和投资社区里的新增热点，减少人工刷新闻的时间。

#### 1.1 数据源接入

MVP 第一批只接 4 类核心数据源，先保证覆盖硬公告、全球新闻主题、金融新闻和社区情绪，不在第一版里铺太多重复源。

**第一批数据源：**

* SEC EDGAR：必接。它不是传统新闻源，但对美股热点发现非常关键，重点关注 8-K、10-Q、10-K、S-1、重大公告、财报披露和风险提示。SEC 的 `data.sec.gov` API 不需要认证，适合做“硬事件源”。官方文档：https://www.sec.gov/search-filings/edgar-application-programming-interfaces
* GDELT DOC API：必接。用于发现宏观、行业、政策、地缘、AI、能源、医药、半导体等主题热度，适合统计新闻数量、来源覆盖、关键词增长率和跨区域传播。文档入口：https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
* Finnhub News：建议第一版接。它提供 market news 和 company news，能按市场分类或 ticker 获取财经新闻，落地成本比直接抓财经网站低，可作为金融新闻主源。官方文档：https://finnhub.io/docs/api/market-news
* Reddit API：建议接，但权重低于 SEC、GDELT 和专业金融新闻。重点观察 `r/stocks`、`r/investing`、`r/wallstreetbets`、`r/SecurityAnalysis` 等社区中 ticker 或主题的讨论量异常增长。官方文档：https://www.reddit.com/dev/api/

**暂缓接入的数据源：**

* Yahoo Finance 新闻：可以作为原型期 RSS 或页面补充，但不要作为核心依赖。公开接口稳定性和使用边界都不适合作为严肃数据管道的唯一来源。
* NewsAPI：暂不作为第一批。它适合开发测试和补充新闻源，但免费额度、延迟和生产使用限制会影响持续轮询。
* Alpha Vantage News & Sentiment：暂不作为第一批。接口方便，但免费额度较紧，更适合后续低频补充或验证情绪标签。
* Polygon/Massive News：质量和覆盖不错，但更偏付费增强源。Phase 2 或愿意直接采购数据时再接，届时可用于替换或增强 Finnhub。
* Google News：可以用于 RSS 或搜索结果辅助，但不应假设存在稳定免费的官方 API。
* X/Twitter：官方 API 成本和限制较高，适合后续阶段再接入。

**第一版落库建议：**

* 所有原始文章、公告和帖子先保存原始标题、摘要、正文片段、发布时间、来源 URL、来源名称、抓取时间和原始 payload。
* 统一抽取 `source_type`，例如 `sec_filing`、`global_news`、`financial_news`、`reddit_post`。
* 对每条记录提取候选 ticker、关键词、主题标签和语言信息。
* 去重时优先使用 URL、标题规范化哈希、发布时间窗口和正文相似度。
* 热点评分时将 SEC 事件权重设高，Reddit 只作为情绪和讨论增长信号，不能单独触发候选股推荐。

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
* Reddit 或社交讨论热度
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
docker compose up -d postgres
.venv/bin/python -m usstock.db.migrations migrate
.venv/bin/python -m usstock.db.migrations status
```

如果项目已安装为可执行命令，也可以使用：

```bash
usstock migrate
usstock status
```

数据库连接默认读取环境变量或 `.env` 中的 `DATABASE_URL`。

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

自动发现流程会写入默认主题库，用主题库替代人工 GDELT query；同步 Finnhub market news 并抽取相关 ticker 和关键词；扫描股票池中的 SEC filings；最后生成每日候选股评分和观察清单。

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

第一版面板只覆盖最常用的内部操作：

* 查看股票池、SEC 公告、GDELT 文章、Finnhub 新闻和最近迁移记录。
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
