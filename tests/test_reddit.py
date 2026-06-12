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

    def test_parse_devvit_single_trigger_post_payload(self) -> None:
        posts = reddit.parse_devvit_posts(
            {
                "source": "devvit",
                "trigger": "onPostSubmit",
                "subreddit": "stocks",
                "matched_keywords": ["AI"],
                "post": {
                    "id": "t3_trigger123",
                    "subredditName": "stocks",
                    "title": "$NVDA AI guidance raises MSFT cloud expectations",
                    "body": "Semiconductor demand remains strong.",
                    "authorName": "trigger_user",
                    "permalink": "/r/stocks/comments/trigger123/example/",
                    "url": "https://www.reddit.com/r/stocks/comments/trigger123/example/",
                    "score": 0,
                    "numberOfComments": 0,
                    "createdAt": "2026-06-12T02:03:04.000Z",
                    "nsfw": False,
                    "spoiler": False,
                    "stickied": False,
                },
            },
            query_uid="query-trigger-post",
            request_url="devvit://trigger/onPostSubmit/t3_trigger123",
            fallback_subreddit="stocks",
            known_tickers={"NVDA", "MSFT"},
        )

        self.assertEqual(len(posts), 1)
        post = posts[0]
        self.assertEqual(post.post_uid, "reddit:t3_trigger123")
        self.assertEqual(post.reddit_id, "trigger123")
        self.assertEqual(post.author_name, "trigger_user")
        self.assertEqual(post.candidate_tickers, ["NVDA", "MSFT"])

    def test_parse_devvit_comment_match_payload(self) -> None:
        comments = reddit.parse_devvit_comments(
            {
                "source": "devvit",
                "trigger": "onCommentSubmit",
                "subreddit": "wallstreetbets",
                "matched_keywords": ["AI", "$NVDA"],
                "post": {
                    "id": "t3_post123",
                    "subredditName": "wallstreetbets",
                    "title": "AI capex thread",
                    "permalink": "/r/wallstreetbets/comments/post123/thread/",
                },
                "comment": {
                    "id": "t1_comment123",
                    "postId": "t3_post123",
                    "parentId": "t3_post123",
                    "subredditName": "wallstreetbets",
                    "body": "$NVDA AI demand still looks wild after earnings.",
                    "authorName": "comment_user",
                    "permalink": "/r/wallstreetbets/comments/post123/thread/comment123/",
                    "score": 12,
                    "createdAt": "2026-06-12T03:04:05.000Z",
                },
            },
            query_uid="query-trigger-comment",
            request_url="devvit://trigger/onCommentSubmit/t1_comment123",
            fallback_subreddit="wallstreetbets",
            known_tickers={"NVDA"},
        )

        self.assertEqual(len(comments), 1)
        comment = comments[0]
        self.assertEqual(comment.comment_uid, "reddit:t1_comment123")
        self.assertEqual(comment.reddit_id, "comment123")
        self.assertEqual(comment.post_fullname, "t3_post123")
        self.assertEqual(comment.parent_fullname, "t3_post123")
        self.assertEqual(comment.author_name, "comment_user")
        self.assertEqual(comment.matched_keywords, ["AI", "$NVDA"])
        self.assertEqual(comment.candidate_tickers, ["NVDA"])
        self.assertEqual(comment.created_utc.tzinfo, timezone.utc)


if __name__ == "__main__":
    unittest.main()
