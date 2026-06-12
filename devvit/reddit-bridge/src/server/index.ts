import { Hono } from 'hono';
import { reddit, settings } from '@devvit/web/server';
import type { TaskRequest } from '@devvit/web/server';
import type { MenuItemRequest, UiResponse } from '@devvit/web/shared';

type Listing = 'new' | 'hot' | 'top' | 'rising' | 'controversial';
type TimeFilter = 'hour' | 'day' | 'week' | 'month' | 'year' | 'all';

type BridgePost = {
  id: string;
  name: string;
  subredditName: string;
  title: string;
  body?: string;
  authorName: string;
  permalink: string;
  url: string;
  score: number;
  numberOfComments: number;
  createdAt: string;
  nsfw: boolean;
  spoiler: boolean;
  stickied: boolean;
  flairText?: string;
};

type BridgePayload = {
  source: 'devvit';
  subreddit: string;
  listing: Listing;
  time_filter?: TimeFilter;
  limit: number;
  request_url: string;
  fetched_at: string;
  posts: BridgePost[];
};

type SyncResult = {
  status: 'ok';
  synced: Record<string, number>;
  total: number;
};

const DEFAULT_SUBREDDITS = ['stocks', 'investing', 'wallstreetbets', 'SecurityAnalysis'];
const VALID_LISTINGS = new Set<Listing>(['new', 'hot', 'top', 'rising', 'controversial']);
const VALID_TIME_FILTERS = new Set<TimeFilter>(['hour', 'day', 'week', 'month', 'year', 'all']);

const app = new Hono();

app.get('/api/health', (c) => c.json({ ok: true }));

app.post('/api/sync', async (c) => {
  const result = await syncAllSubreddits();
  return c.json<SyncResult>(result);
});

app.post('/internal/cron/sync-reddit-defaults', async (c) => {
  await c.req.json<TaskRequest>().catch(() => undefined);
  const result = await syncAllSubreddits();
  return c.json({ status: 'ok', result });
});

app.post('/internal/menu/sync-reddit', async (c) => {
  await c.req.json<MenuItemRequest>().catch(() => undefined);
  const result = await syncAllSubreddits();
  return c.json<UiResponse>({
    showToast: {
      text: `usstock synced ${result.total} Reddit posts`,
      appearance: 'success',
    },
  });
});

async function syncAllSubreddits(): Promise<SyncResult> {
  const config = await loadConfig();
  const synced: Record<string, number> = {};
  let total = 0;

  for (const subredditName of config.subreddits) {
    const payload = await fetchSubredditPayload({
      subredditName,
      listing: config.listing,
      timeFilter: config.timeFilter,
      limit: config.limit,
    });
    await pushToUsstock(config.webhookUrl, config.webhookSecret, payload);
    synced[subredditName] = payload.posts.length;
    total += payload.posts.length;
  }

  return { status: 'ok', synced, total };
}

async function loadConfig() {
  const [
    webhookUrlValue,
    webhookSecretValue,
    subredditNamesValue,
    listingValue,
    timeFilterValue,
    limitValue,
  ] = await Promise.all([
    settings.get('usstockWebhookUrl'),
    settings.get('usstockWebhookSecret'),
    settings.get('subredditNames'),
    settings.get('listing'),
    settings.get('timeFilter'),
    settings.get('limit'),
  ]);

  const webhookUrl = String(webhookUrlValue || '').trim();
  const webhookSecret = String(webhookSecretValue || '').trim();
  if (!webhookUrl) {
    throw new Error('Missing usstockWebhookUrl setting');
  }
  if (!webhookSecret) {
    throw new Error('Missing usstockWebhookSecret setting');
  }

  const subreddits = String(subredditNamesValue || '')
    .split(',')
    .map((item) => normalizeSubredditName(item))
    .filter((item): item is string => Boolean(item));
  const listing = normalizeListing(listingValue);
  const timeFilter = normalizeTimeFilter(timeFilterValue);
  const limit = normalizeLimit(limitValue);

  return {
    webhookUrl,
    webhookSecret,
    subreddits: subreddits.length > 0 ? subreddits : DEFAULT_SUBREDDITS,
    listing,
    timeFilter,
    limit,
  };
}

async function fetchSubredditPayload(options: {
  subredditName: string;
  listing: Listing;
  timeFilter: TimeFilter;
  limit: number;
}): Promise<BridgePayload> {
  const fetchOptions = {
    subredditName: options.subredditName,
    limit: options.limit,
    pageSize: Math.min(options.limit, 100),
  };
  const fetchOptionsWithTimeframe = {
    ...fetchOptions,
    timeframe: options.timeFilter,
  };

  const listing =
    options.listing === 'hot'
      ? reddit.getHotPosts(fetchOptionsWithTimeframe)
      : options.listing === 'top'
        ? reddit.getTopPosts(fetchOptionsWithTimeframe)
        : options.listing === 'rising'
          ? reddit.getRisingPosts(fetchOptions)
          : options.listing === 'controversial'
            ? reddit.getControversialPosts(fetchOptionsWithTimeframe)
            : reddit.getNewPosts(fetchOptions);

  const posts = await listing.all();
  const bridgePosts = posts.slice(0, options.limit).map(toBridgePost);
  return {
    source: 'devvit',
    subreddit: options.subredditName,
    listing: options.listing,
    time_filter: options.timeFilter,
    limit: options.limit,
    request_url: buildRequestUrl(options),
    fetched_at: new Date().toISOString(),
    posts: bridgePosts,
  };
}

async function pushToUsstock(
  webhookUrl: string,
  webhookSecret: string,
  payload: BridgePayload,
) {
  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${webhookSecret}`,
      'Content-Type': 'application/json',
      'X-Usstock-Webhook-Source': 'devvit',
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const body = await response.text().catch(() => '');
    throw new Error(`usstock webhook failed ${response.status}: ${body}`);
  }
}

function toBridgePost(post: any): BridgePost {
  const id = String(post.id || '');
  return {
    id,
    name: id,
    subredditName: String(post.subredditName || ''),
    title: String(post.title || ''),
    body: post.body ? String(post.body) : undefined,
    authorName: String(post.authorName || ''),
    permalink: String(post.permalink || ''),
    url: String(post.url || ''),
    score: Number(post.score || 0),
    numberOfComments: Number(post.numberOfComments || 0),
    createdAt: post.createdAt instanceof Date
      ? post.createdAt.toISOString()
      : new Date(post.createdAt || Date.now()).toISOString(),
    nsfw: Boolean(post.nsfw),
    spoiler: Boolean(post.spoiler),
    stickied: Boolean(post.stickied),
    flairText: extractFlairText(post.flair),
  };
}

function extractFlairText(flair: unknown): string | undefined {
  if (!flair || typeof flair !== 'object') {
    return undefined;
  }
  const text = (flair as { text?: unknown; label?: unknown }).text
    ?? (flair as { text?: unknown; label?: unknown }).label;
  return text ? String(text) : undefined;
}

function normalizeSubredditName(value: unknown): string | undefined {
  const text = String(value || '').trim().replace(/^\/?r\//i, '').replace(/^\/|\/$/g, '');
  return /^[A-Za-z0-9_]+$/.test(text) ? text : undefined;
}

function normalizeListing(value: unknown): Listing {
  const listing = String(value || 'new').trim().toLowerCase() as Listing;
  return VALID_LISTINGS.has(listing) ? listing : 'new';
}

function normalizeTimeFilter(value: unknown): TimeFilter {
  const timeFilter = String(value || 'day').trim().toLowerCase() as TimeFilter;
  return VALID_TIME_FILTERS.has(timeFilter) ? timeFilter : 'day';
}

function normalizeLimit(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return 50;
  }
  return Math.min(Math.floor(parsed), 100);
}

function buildRequestUrl(options: {
  subredditName: string;
  listing: Listing;
  timeFilter: TimeFilter;
  limit: number;
}) {
  const params = new URLSearchParams({ limit: String(options.limit) });
  if (options.listing === 'top' || options.listing === 'controversial') {
    params.set('t', options.timeFilter);
  }
  return `devvit://reddit/r/${options.subredditName}/${options.listing}?${params}`;
}

export default app;
