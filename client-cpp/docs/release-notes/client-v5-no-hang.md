# Release note: client version 5 — no hangs, no silent fallback

The client now announces `clientVersion = 5` on Connect. Version 5 means the
client handles the broker's error answers to malformed or unservable requests
instead of waiting for its timeout.

## 1. Malformed-request completion requires client >= 5

The broker answers a request whose header decodes but whose body does not with
the request's own response type and an error code. The pending operation
completes with that error.

| Answer | Meaning | Client 5 behaviour |
|---|---|---|
| `InvalidRequest` (39) | the broker could not decode the request | non-retriable: the operation fails; a group consumer is refused (`GroupFailReason::RefusedByBroker`); a transaction op fails without a retry |
| `Internal` (6) on JoinGroup / SyncGroup / Heartbeat | the coordinator could not serve it | re-join with backoff (20 ms doubling to 1 s), refused after 8 attempts |
| ViewRow status 5 / 6 / 7 | InvalidRequest / Busy / RowTooLarge | the read completes with that status; only Busy is retriable |

Older clients (< 5) re-join forever on InvalidRequest / Internal JoinGroup and
SyncGroup answers, so the broker answers those clients `ProtocolVersion`
instead, which they already treat as non-retriable. Other request types still
reach an older client as InvalidRequest; it completes control operations with
`BrokerError` and times out the rest as before.

## 2. A Subscribe error now finishes the subscription

A `SubscribeResponse` that carries an error (`UnknownTopic`,
`UnknownPartition`, `InvalidRequest`) now finishes the subscription:
`Subscription::failReason() == SubscriptionFailReason::RejectedByBroker`, and
`Subscription::brokerErrorCode()` holds the code. Earlier clients bound such a
subscription as if it succeeded; it stayed active and never delivered.

**Applications must re-subscribe** after `RejectedByBroker`, for example after
creating the topic.

## 3. ABI change: SubscriptionStatus

`SubscriptionStatus` gained a `brokerErrorCode` field and
`SubscriptionFailReason` gained `RejectedByBroker = 2`. Rebuild everything that
includes `rune/client/subscriber/Subscription.h`.

## 4. Other behaviour changes

- A consumer group's OffsetFetch answer is matched by correlation id. A
  whole-request rejection refuses the consumer. A retriable per-entry error is
  re-fetched (paced) at most 8 times, then the consumer is refused.
- A Heartbeat error outside the rebalance set no longer leaves the member
  Stable: the member re-joins with backoff, or is refused on InvalidRequest.
- An ErrorResponse fails a pending `connect()` only when it answers that
  connect (or ends the session); an error answering another request no longer
  fails the connect.
