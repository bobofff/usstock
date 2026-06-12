# usstock Devvit Reddit Bridge

这个子项目负责在 Devvit 运行时读取 Reddit 帖子，并实时匹配新帖子/新评论关键词，然后把标准化 JSON 推送回 `usstock` 后端：

```text
Devvit scheduler/menu -> Reddit API -> usstock webhook -> reddit_posts
Devvit onPostSubmit/onCommentSubmit -> keyword match -> usstock webhook -> reddit_posts/reddit_comments
```

## 使用前配置

1. 把 `devvit.json` 里的 HTTP allow-list 域名 `your-domain.example` 改成你的公开 HTTPS 后端域名，只写 hostname，不要写协议和路径。不要使用 `127.0.0.1`，Devvit 托管运行时访问不到你的本机服务。
2. 在主项目 `.env` 里配置同一个密钥：

```bash
REDDIT_DEVVIT_WEBHOOK_SECRET=replace-with-a-long-random-secret
```

3. 启动或部署主项目 admin server，并确保 Devvit 能访问：

```text
https://your-domain.example/api/reddit/devvit/posts
https://your-domain.example/api/reddit/devvit/matches
```

4. 在主项目执行数据库迁移，确保 `reddit_posts` 和 `reddit_comments` 都存在：

```bash
.venv/bin/python -m usstock.db.migrations migrate
```

5. 在本目录安装依赖并登录 Devvit：

```bash
npm install
npm run login
npm run build
npm run playtest
```

6. 在 Devvit settings 里设置：

```text
usstockWebhookUrl=https://your-domain.example/api/reddit/devvit/posts
usstockMatchWebhookUrl=https://your-domain.example/api/reddit/devvit/matches
usstockWebhookSecret=<和 REDDIT_DEVVIT_WEBHOOK_SECRET 相同>
subredditNames=stocks,investing,wallstreetbets,SecurityAnalysis
keywordPatterns=$NVDA,$MSFT,$AAPL,$TSLA,AI,semiconductor,earnings,guidance,Fed,rates,inflation
```

## 运行方式

`devvit.json` 默认每 30 分钟跑一次 `sync-reddit-defaults`。也可以在 subreddit 菜单里触发 `Sync usstock Reddit signals`。

默认同步：

```text
stocks, investing, wallstreetbets, SecurityAnalysis
```

默认 listing 是 `new`，每个 subreddit 最多 50 条。可以在 Devvit 安装设置里调整 subreddit、listing、time filter 和 limit。

实时关键词匹配由 `onPostSubmit` 和 `onCommentSubmit` 自动触发。`keywordPatterns` 支持逗号或换行分隔，匹配方式是大小写不敏感的包含匹配。命中后：

- 帖子推送到 `/api/reddit/devvit/matches` 并写入 `reddit_posts`。
- 评论推送到 `/api/reddit/devvit/matches` 并写入 `reddit_comments`。
- payload 会带上 `matched_keywords`，用于审计是哪几个关键词触发了入库。
