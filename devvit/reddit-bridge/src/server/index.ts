import { Hono } from 'hono';
import { reddit, settings } from '@devvit/web/server';
import type { TaskRequest } from '@devvit/web/server';
import type {
  MenuItemRequest,
  OnCommentSubmitRequest,
  OnPostSubmitRequest,
  TriggerResponse,
  UiResponse,
} from '@devvit/web/shared';

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

type BridgeComment = {
  id: string;
  name: string;
  postId: string;
  parentId?: string;
  subredditName: string;
  body: string;
  authorName: string;
  permalink: string;
  score: number;
  createdAt: string;
};

type RealtimeTrigger = 'onPostSubmit' | 'onCommentSubmit';

type MatchPayload = {
  source: 'devvit';
  trigger: RealtimeTrigger;
  subreddit: string;
  matched_keywords: string[];
  request_url: string;
  received_at: string;
  post?: BridgePost;
  comment?: BridgeComment;
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

app.post('/internal/triggers/on-post-submit', async (c) => {
  const input = await c.req.json<OnPostSubmitRequest>();
  const config = await loadConfig();
  const post = input.post;
  const subredditName = normalizeSubredditName(input.subreddit?.name)
    || normalizeSubredditName((post as any)?.subredditName)
    || '';
  const text = [post?.title, post?.selftext].filter(Boolean).join('\n');
  const matchedKeywords = findMatchedKeywords(text, config.keywordPatterns);

  if (post && subredditName && matchedKeywords.length > 0) {
    const bridgePost = toBridgePost(post, subredditName, input.author?.name);
    await pushToUsstock(config.matchWebhookUrl, config.webhookSecret, {
      source: 'devvit',
      trigger: 'onPostSubmit',
      subreddit: subredditName,
      matched_keywords: matchedKeywords,
      request_url: buildTriggerRequestUrl('onPostSubmit', bridgePost.name),
      received_at: new Date().toISOString(),
      post: bridgePost,
    });
  }

  return c.json<TriggerResponse>({});
});

app.post('/internal/triggers/on-comment-submit', async (c) => {
  const input = await c.req.json<OnCommentSubmitRequest>();
  const config = await loadConfig();
  const comment = input.comment;
  const subredditName = normalizeSubredditName(input.subreddit?.name)
    || normalizeSubredditName((comment as any)?.subredditName)
    || '';
  const matchedKeywords = findMatchedKeywords(comment?.body || '', config.keywordPatterns);

  if (comment && subredditName && matchedKeywords.length > 0) {
    const bridgeComment = toBridgeComment(comment, subredditName, input.author?.name);
    const bridgePost = input.post
      ? toBridgePost(input.post, subredditName)
      : undefined;
    await pushToUsstock(config.matchWebhookUrl, config.webhookSecret, {
      source: 'devvit',
      trigger: 'onCommentSubmit',
      subreddit: subredditName,
      matched_keywords: matchedKeywords,
      request_url: buildTriggerRequestUrl('onCommentSubmit', bridgeComment.name),
      received_at: new Date().toISOString(),
      post: bridgePost,
      comment: bridgeComment,
    });
  }

  return c.json<TriggerResponse>({});
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
    matchWebhookUrlValue,
    webhookSecretValue,
    subredditNamesValue,
    listingValue,
    timeFilterValue,
    limitValue,
    keywordPatternsValue,
  ] = await Promise.all([
    settings.get('usstockWebhookUrl'),
    settings.get('usstockMatchWebhookUrl'),
    settings.get('usstockWebhookSecret'),
    settings.get('subredditNames'),
    settings.get('listing'),
    settings.get('timeFilter'),
    settings.get('limit'),
    settings.get('keywordPatterns'),
  ]);

  const webhookUrl = String(webhookUrlValue || '').trim();
  const matchWebhookUrl = String(matchWebhookUrlValue || '').trim()
    || webhookUrl.replace(/\/posts\/?$/, '/matches');
  const webhookSecret = String(webhookSecretValue || '').trim();
  if (!webhookUrl) {
    throw new Error('Missing usstockWebhookUrl setting');
  }
  if (!matchWebhookUrl) {
    throw new Error('Missing usstockMatchWebhookUrl setting');
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
  const keywordPatterns = normalizeKeywordPatterns(keywordPatternsValue);

  return {
    webhookUrl,
    matchWebhookUrl,
    webhookSecret,
    subreddits: subreddits.length > 0 ? subreddits : DEFAULT_SUBREDDITS,
    listing,
    timeFilter,
    limit,
    keywordPatterns,
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
  const bridgePosts = posts.slice(0, options.limit).map((post) => toBridgePost(post));
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
  payload: BridgePayload | MatchPayload,
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

function toBridgePost(
  post: any,
  fallbackSubredditName?: string,
  fallbackAuthorName?: string,
): BridgePost {
  const id = String(post.name || post.fullname || post.id || post.postId || '');
  const permalink = String(post.permalink || '');
  const url = String(post.url || permalink || '');
  const score = Number(post.score ?? Number.NaN);
  const upvotes = Number(post.upvotes ?? 0);
  const downvotes = Number(post.downvotes ?? 0);
  return {
    id,
    name: id,
    subredditName: String(post.subredditName || fallbackSubredditName || ''),
    title: String(post.title || ''),
    body: post.body || post.selftext ? String(post.body || post.selftext) : undefined,
    authorName: String(post.authorName || post.author || fallbackAuthorName || ''),
    permalink,
    url,
    score: Number.isFinite(score) ? score : upvotes - downvotes,
    numberOfComments: Number(post.numberOfComments ?? post.numComments ?? 0),
    createdAt: normalizeTimestamp(post.createdAt ?? post.created_utc ?? Date.now()),
    nsfw: Boolean(post.nsfw ?? post.over_18),
    spoiler: Boolean(post.spoiler ?? post.isSpoiler),
    stickied: Boolean(post.stickied ?? post.isSticky),
    flairText: extractFlairText(post.flair ?? post.linkFlair),
  };
}

function toBridgeComment(
  comment: any,
  fallbackSubredditName?: string,
  fallbackAuthorName?: string,
): BridgeComment {
  const id = String(comment.name || comment.fullname || comment.id || comment.commentId || '');
  const permalink = String(comment.permalink || '');
  const score = Number(comment.score ?? Number.NaN);
  const upvotes = Number(comment.upvotes ?? 0);
  const downvotes = Number(comment.downvotes ?? 0);
  return {
    id,
    name: id,
    postId: String(comment.postId || comment.post_id || ''),
    parentId: comment.parentId ? String(comment.parentId) : undefined,
    subredditName: String(comment.subredditName || fallbackSubredditName || ''),
    body: String(comment.body || ''),
    authorName: String(comment.authorName || comment.author || fallbackAuthorName || ''),
    permalink,
    score: Number.isFinite(score) ? score : upvotes - downvotes,
    createdAt: normalizeTimestamp(comment.createdAt ?? comment.created_utc ?? Date.now()),
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

function normalizeTimestamp(value: unknown): string {
  if (value instanceof Date) {
    return value.toISOString();
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    const millis = value > 9_999_999_999 ? value : value * 1000;
    return new Date(millis).toISOString();
  }
  if (typeof value === 'string' && value.trim()) {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toISOString();
    }
  }
  return new Date().toISOString();
}

function normalizeKeywordPatterns(value: unknown): string[] {
  const text = String(value || '');
  const keywords = text
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
  const seen = new Set<string>();
  const unique: string[] = [];
  for (const keyword of keywords) {
    const key = keyword.toLowerCase();
    if (!seen.has(key)) {
      seen.add(key);
      unique.push(keyword);
    }
  }
  return unique;
}

function findMatchedKeywords(text: string, keywords: string[]): string[] {
  const haystack = text.toLowerCase();
  return keywords.filter((keyword) => haystack.includes(keyword.toLowerCase()));
}

function buildTriggerRequestUrl(trigger: RealtimeTrigger, thingId: string): string {
  const id = encodeURIComponent(thingId || 'unknown');
  return `devvit://trigger/${trigger}/${id}`;
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
