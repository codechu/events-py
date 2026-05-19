# Security policy

`codechu-events` is a small in-process pub/sub library. It does not
touch the network, the filesystem, or `subprocess`. The threat model
is therefore narrow — but bugs in concurrent code can still create
denial-of-service or correctness issues, and we treat those as
security-relevant.

## Supported versions

| Version | Supported |
|---|:---:|
| `main` branch | ✅ |
| Latest minor release (0.x) | ✅ |
| Older releases | ❌ |

Pre-1.0.0 period — only the latest minor receives security fixes.

## Reporting a vulnerability

### Preferred path — GitHub Security Advisory (private)

Open a **private** advisory at
[github.com/codechu/codechu-events-py/security/advisories/new](https://github.com/codechu/codechu-events-py/security/advisories/new).
The disclosure stays non-public until a fix lands, and a CVE can be
requested automatically.

### Alternative — Email

Write to `security@codechu.com`.

## Scope — what to report

**In scope:**

- **Unbounded memory growth** — a publisher pattern that lets queues
  grow past `QUEUE_MAX`, or a subscriber leak past
  `MAX_SUBSCRIBERS`.
- **Deadlock or livelock** in `Bus` under realistic publish /
  subscribe / unsubscribe interleavings.
- **Lost events** when the documented contract says they should be
  delivered (matching filter, queue has room).
- **Cross-bus leakage** — an event published on bus A reaching a
  subscriber on bus B.
- **Subscription-filter bypass** — events delivered to a subscriber
  whose `matches()` rejected them.

**Out of scope:**

- Slow consumers experiencing drops — this is documented, by design.
- Dependency vulnerabilities in `pytest` / `ruff` (dev-only).
- Misuse of the API (e.g. publishing on an unsubscribed bus and
  expecting persistence — there is no persistence).

## Process

Reports are reviewed on a best-effort basis — no fixed SLA. We aim
for coordinated disclosure within **90 days** of the report,
extendable by mutual agreement if a fix is non-trivial.

Public disclosure is coordinated after the fix is released
(together with the reporter).

## Public disclosure

Once a confirmed fix is released:

- A summary is added to the CHANGELOG under the `### Security`
  category (with the reporter's name if they want credit).
- A GitHub Security Advisory is published.
- If a CVE was assigned, its number is referenced.
