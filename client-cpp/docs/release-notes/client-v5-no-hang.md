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
| `Internal` (6) on JoinGroup / SyncGroup / Heartbeat | the coordinator could not serve it | re-join with backoff (20 ms doubling to 1 s); after 8 such re-joins without a healthy Heartbeat in between the consumer finishes with `GroupFailReason::RetryBudgetExhausted` |
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
  whole-request rejection refuses the consumer (`RefusedByBroker`). A
  retriable per-entry error is re-fetched (paced) at most 8 times, then the
  consumer finishes with `RetryBudgetExhausted`. A per-entry `NotCoordinator`
  (or another rebalance code: a coordinator failover) makes the member re-join,
  which rediscovers the coordinator, without spending that budget. After 8
  such re-joins in a row with no OffsetFetch served in between, the consumer
  finishes with `RetryBudgetExhausted`.
- `GroupFailReason::RetryBudgetExhausted` (3) is new. It marks a transient
  cause, so the application may recreate the consumer. `RefusedByBroker` marks
  a non-retriable answer; recreating that consumer will be refused again.
- A Heartbeat error outside the rebalance set no longer leaves the member
  Stable: the member re-joins with backoff, or is refused on InvalidRequest.
- When the client's own response timeout declares the session lost (no
  answer and no Pong within `responseTimeoutNs`), every pending control
  operation completes `SessionLost` at once, as on a broker `SessionExpired`.
  Before, those operations waited for their own deadline and completed
  `Timeout`.
- Older clients (< 5) stay Stable on a Heartbeat answer outside the rebalance
  set. The broker therefore answers them `UnknownMember` instead of `Internal`
  or `InvalidRequest` on Heartbeat, SyncGroup and OffsetFetch, and evicts their
  member, so they re-join. A JoinGroup or SyncGroup the broker cannot decode
  is still answered `ProtocolVersion` to them, as above.
- In a TLS handshake, a `HandshakeContinueResponse` with a code other than
  `HandshakeFailed` (for example `Internal`, when the broker's pending-connect
  table was full) is a retriable connect failure. Before, every non-OK code
  ended the session as a terminal `HandshakeFailed`.
- An ErrorResponse fails a pending `connect()` only when it answers that
  connect (or ends the session); an error answering another request no longer
  fails the connect.
