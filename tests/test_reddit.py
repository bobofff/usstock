from __future__ import annotations

import unittest
from datetime import timezone

from usstock.data import reddit


class RedditParsingTest(unittest.TestCase):
    def test_parse_post_payload_normalizes_fields_and_tickers(self) -> None:
        post = reddit.parse_post_payload(
            {
                "kind": "t3",
                "data": {
                    "id": "abc123",
                    "name": "t3_abc123",
                    "subreddit": "stocks",
                    "title": "$NVDA and MSFT rally on AI datacenter demand",
                    "selftext": "Watching NVDA after earnings.",
                    "author": "example_user",
                    "permalink": "/r/stocks/comments/abc123/example/",
                    "url": "https://www.reddit.com/r/stocks/comments/abc123/example/",
                    "score": 120,
                    "upvote_ratio": 0.91,
                    "num_comments": 34,
                    "created_utc": 1_717_200_000,
                    "link_flair_text": "Discussion",
                },
            },
            query_uid="query-1",
            request_url="https://oauth.reddit.com/r/stocks/new.json?limit=25",
            known_tickers={"NVDA", "MSFT"},
        )

        self.assertIsNotNone(post)
        assert post is not None
        self.assertEqual(post.post_uid, "reddit:t3_abc123")
        self.assertEqual(post.permalink_url, "https://www.reddit.com/r/stocks/comments/abc123/example/")
        self.assertIsNone(post.external_url)
        self.assertEqual(post.candidate_tickers, ["NVDA", "MSFT"])
        self.assertIn("datacenter", post.candidate_keywords)
        self.assertEqual(post.source_type, "reddit_post")
        self.assertEqual(post.created_utc.tzinfo, timezone.utc)

    def test_client_builds_oauth_listing_url_without_credentials(self) -> None:
        client = reddit.RedditClient(
            client_id="client-id",
            client_secret="client-secret",
            user_agent="usstock-test/0.1 by test",
            base_url="https://example.test",
            oauth_url="https://auth.example.test/token",
            requests_per_second=1000,
        )

        url = client.build_url(
            "r/stocks/new.json",
            {"limit": "25", "raw_json": "1"},
        )

        self.assertEqual(url, "https://example.test/r/stocks/new.json?limit=25&raw_json=1")
        self.assertNotIn("client-id", url)
        self.assertNotIn("client-secret", url)

    def test_parse_devvit_payload_reuses_existing_post_model(self) -> None:
        posts = reddit.parse_devvit_posts(
            {
                "subreddit": "stocks",
                "listing": "new",
                "limit": 25,
                "posts": [
                    {
                        "id": "t3_dev123",
                        "subredditName": "stocks",
                        "title": "$NVDA demand and MSFT cloud capex",
                        "body": "Watching NVDA after datacenter earnings.",
                        "authorName": "devvit_user",
                        "permalink": "/r/stocks/comments/dev123/example/",
                        "url": "https://www.reddit.com/r/stocks/comments/dev123/example/",
                        "score": 42,
                        "numberOfComments": 9,
                        "createdAt": "2026-06-12T01:02:03.000Z",
                        "nsfw": False,
                        "spoiler": False,
                        "stickied": False,
                        "flair": {"text": "Discussion"},
                    }
                ],
            },
            query_uid="query-devvit",
            request_url="devvit://reddit/r/stocks/new?limit=25",
            fallback_subreddit="stocks",
            known_tickers={"NVDA", "MSFT"},
        )

        self.assertEqual(len(posts), 1)
        post = posts[0]
        self.assertEqual(post.post_uid, "reddit:t3_dev123")
        self.assertEqual(post.reddit_id, "dev123")
        self.assertEqual(post.author_name, "devvit_user")
        self.assertEqual(post.comment_count, 9)
        self.assertEqual(post.link_flair_text, "Discussion")
        self.assertEqual(post.candidate_tickers, ["NVDA", "MSFT"])
        self.assertEqual(post.created_utc.tzinfo, timezone.utc)


if __name__ == "__main__":
    unittest.main()
