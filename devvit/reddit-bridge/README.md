# usstock Devvit Reddit Bridge

这个子项目负责在 Devvit 运行时读取 Reddit 帖子，然后把标准化 JSON 推送回 `usstock` 后端：

```text
Devvit scheduler/menu -> Reddit API -> usstock webhook -> reddit_posts
```

## 使用前配置

1. 把 `devvit.json` 里的 HTTP allow-list 域名从 `example.com` 改成你的公开 HTTPS 后端域名，只写 hostname，不要写协议和路径。
2. 在主项目 `.env` 里配置同一个密钥：

```bash
REDDIT_DEVVIT_WEBHOOK_SECRET=replace-with-a-long-random-secret
```

3. 启动或部署主项目 admin server，并确保 Devvit 能访问：

```text
https://your-domain.example/api/reddit/devvit/posts
```

4. 在本目录安装依赖并登录 Devvit：

```bash
npm install
npm run login
npm run build
npm run playtest
```

5. 在 Devvit settings 里设置：

```text
usstockWebhookUrl=https://your-domain.example/api/reddit/devvit/posts
usstockWebhookSecret=<和 REDDIT_DEVVIT_WEBHOOK_SECRET 相同>
```

## 运行方式

`devvit.json` 默认每 30 分钟跑一次 `sync-reddit-defaults`。也可以在 subreddit 菜单里触发 `Sync usstock Reddit signals`。

默认同步：

```text
stocks, investing, wallstreetbets, SecurityAnalysis
```

默认 listing 是 `new`，每个 subreddit 最多 50 条。可以在 Devvit 安装设置里调整 subreddit、listing、time filter 和 limit。

