# TikTok source contracts

Verified against official documentation on 2026-10-22. These are engineering contracts, not proof that this application's account, scopes, retention, or intended use are approved. No live credentials were available.

## Display API

The [video-list contract](https://developers.tiktok.com/docs/en/tiktok-api-v2-video-list) specifies POST `https://open.tiktokapis.com/v2/video/list/`, a user bearer token, and `video.list`. It lists the authorized user's public posts, newest first. Body `max_count` defaults to 10 and cannot exceed 20. Continue only when `data.has_more` is true, passing `data.cursor`; this cursor is an integer UTC timestamp in milliseconds. Request fields through the `fields` query parameter.

Illustrative requests, using placeholders rather than real credentials:

```http
POST /v2/video/list/?fields=id,create_time,title,view_count,like_count,comment_count,share_count
Authorization: Bearer <user_access_token>
Content-Type: application/json

{"max_count":20}
```

The [video-query contract](https://developers.tiktok.com/docs/en/tiktok-api-v2-video-query) checks that requested videos belong to the authorized user; arbitrary public-video IDs are not a discovery mechanism. POST `/v2/video/query/` with the same scope, at most 20 IDs, and a body such as:

```json
{"filters":{"video_ids":["7100000000000000001"]}}
```

Both endpoints return `data.videos` and an `error` object whose successful `code` is `ok`. The list endpoint additionally returns `data.cursor` and `data.has_more`; query has no documented pagination.

The [video object](https://developers.tiktok.com/docs/en/tiktok-api-v2-video-object) includes ID, publication time, description/title, duration, dimensions, URLs, engagement counters, and an AI-content flag. `create_time` is seconds, unlike the list cursor. Cover URLs expire after six hours. Neither creator identity nor audience geography is a field of this object; retain the authorized-account context separately. No retention, completion, swipe, purchase, or per-view event series is documented by this contract.

The [published rate-limit page](https://developers.tiktok.com/docs/en/tiktok-api-v2-rate-limit) gives separate defaults of 600 requests per one-minute sliding window for list and query. Exceeding a limit can return HTTP 429 with `rate_limit_exceeded`. Treat those as documented defaults, not evidence of this account's effective grant; configure conservative shared budgets and react to actual responses.

## Commercial Content Library: advertisements

The [ad-query contract](https://developers.tiktok.com/docs/en/commercial-content-api-query-ads) specifies POST `/v2/research/adlib/ad/query/`, scope `research.adlib.basic`, and a client bearer token. The documented page maximum is **10**, not 50. Required filters include `ad_published_date_range`; its minimum must fall within one year. Use `YYYYMMDD` strings. A text query has at most 50 characters.

```json
{
  "filters": {
    "ad_published_date_range":{"min":"20261001","max":"20261021"},
    "country_code_list":["FR"]
  },
  "search_term":"desk lamp",
  "max_count":10
}
```

Response records are `data.ads`, each containing `ad` and `advertiser`. Select fields through names such as `ad.id`, `ad.first_shown_date`, `ad.last_shown_date`, `ad.reach`, and `advertiser.business_id`. Reach is user reach, not organic-video view count; preserve source strings and precision.

Continuation uses returned `data.search_id` while `data.has_more` is true. The [pagination guide](https://developers.tiktok.com/docs/en/commercial-content-api-getting-started) confirms passing `search_id` in subsequent request bodies. Restart without it when filters or search terms change. No offset cursor is documented.

## Commercial Content Library: disclosed commercial posts

The [commercial-content contract](https://developers.tiktok.com/docs/en/commercial-content-api-query-commercial-content) specifies POST `/v2/research/adlib/commercial_content/query/` with the same client token and scope. Its page maximum is **50** and default is 10. Required `content_published_date_range.min` is after 2022-10-01. Optional filters include creator usernames and creator country; creator country is not audience geography. The documented country coverage is EEA, excluding UK and Switzerland.

```json
{
  "filters": {
    "content_published_date_range":{"min":"20261001","max":"20261021"},
    "creator_country_code":"FR"
  },
  "max_count":50
}
```

Returned records are `data.commercial_contents`, with source ID, date/timestamp, disclosure label, sponsor brands, creator username, and video references where requested. Continuation uses `search_id` and `has_more`. These records do not document organic engagement counters, viewer retention, or sales.

## Authentication and authorization

[Client-token management](https://developers.tiktok.com/docs/en/client-access-token-management) uses POST `/v2/oauth/token/`, form encoding, `client_key`, `client_secret`, and `grant_type=client_credentials`. Response keys include `access_token`, `expires_in`, and `token_type`; the documented lifetime is two hours. Refresh using the credential grant rather than a user refresh token.

[User-token management](https://developers.tiktok.com/docs/en/oauth-user-access-token-management) uses the same token endpoint with the authorization-code grant, or the refresh-token grant for subsequent renewal. Persist the returned scope and expiry and atomically replace the refresh token when it changes. The documented user-token lifetime is 24 hours. Tokens and client secrets belong in a secret store, never raw observation objects or diagnostic output.

The [CCL product page](https://developers.tiktok.com/products/commercial-content-api/) requires an approved application and initially describes EU advertisement coverage; account location does not establish content-market coverage. The CCL endpoint pages examined do not publish a universal daily request quota. Do not apply the separate Research API's daily allowance to CCL or Display.

The current [Developer Terms](https://t.tiktok.com/legal/page/global/tik-tok-developer-terms-of-service/en), last modified 2025-12-26, define CCL Purposes around supervision and research into advertising-related risks. Their commercial-use restriction and purpose-specific exemptions mean that an API key alone does not establish permission for this product-trend use. Keep the live intended-use capability unapproved until applicable written authorization and conditions are verified. This report does not determine the application's legal eligibility.

## Document inconsistencies and defensive parsing

The official CCL documentation contains inconsistent examples:

- `has_more` is typed boolean, but examples also encode `"true"` as a string. Accept booleans and exact `"true"`/`"false"` strings explicitly; reject other values. Never use generic string truthiness.
- Ad reach examples alternate `unique_user_seen` and `unique_users_seen`.
- Advertiser examples contain `buisness_id`, while the declared field is `business_id`; funding field examples use `paid_by`, while requested-field lists use `paid_for_by`.
- Ad country examples disagree between `country_code` and the schema's `country_code_list`. Prefer the declared schema and verify against actual authorized responses.
- Commercial-post examples use a `videos` array while one type table says singular `video`; source IDs are described numerically but an example uses a nonnumeric ID. Preserve identifiers as strings and quarantine ambiguous incompatible shapes rather than truncating them.

These inconsistencies are observations about the cited contracts, not permission to silently synthesize fields. Preserve original payloads, record the normalization version and chosen aliases, surface unknown values, and add actual approved response fixtures after live acceptance.

## Live acceptance still required

Record the actual endpoint version, account/project scope, allowed purpose, market coverage, relevant retention/deletion requirements, response field precision, observed throttling, and credential revocation behavior before enabling live collection. The Research video API is intentionally outside this commercial pipeline; its endpoints, eligibility, and quotas are not substitutes for the contracts above.
