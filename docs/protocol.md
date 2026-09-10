---
layout: default
---

# The Bedrock Wire Protocol (BWP)

**Status:** Informational. Describes existing behaviour.

**Verified against:** commit `b1e7fcf40050d29aea40f38409d6e559ef970f90` (2026-08-06).

## Abstract

Bedrock speaks a text protocol that resembles HTTP/1.1 but is not HTTP/1.1. This
document specifies the message format, framing, header conventions, status
codes, and connection semantics, for three uses of the same envelope:

1. The client protocol;
2. Command escalation between nodes; and
3. Cluster replication.

This document describes the protocol as deployed. Section 15 lists behaviour
that appears to be defective; that behaviour is documented but is not normative,
and implementations should not rely on it.

---

## The Protocol in Brief

Everything needed to write a working client. The rest of the document is
reference detail.

A message is a method line, some `name: value` fields, a blank line, and an
optional body. A request:

```
Status
Content-Length: 0

```

A response has the same shape, with a status in place of the method line:

```
200 OK
Content-Length: 55
commitCount: 1409377
nodeName: bedrock1

{"isLeader":"true","state":"LEADING","version":"2.1.0"}
```

Six rules cover almost all of it:

1. **End every line with CRLF.** A receiver also accepts a bare LF, which is
   what typing into `netcat` produces.
2. **A blank line ends the fields.** Everything after it is the body.
3. **`Content-Length` gives the body length in octets.** Send it, and make it
   exact, on any connection you will reuse. See section 4.4; this is the rule
   most often got wrong.
4. **Field names are case-insensitive and must not contain a colon.** Field
   values are backslash-escaped, so a value can carry a newline as `\n` and
   therefore a whole JSON document on one line.
5. **The status is the leading integer of the response's first line.** `2xx` is
   success. The words after the integer are not decorative; see section 8.4.
6. **Connections are persistent.** Send `Connection: close` when finished, or
   the server waits for another request.

Two things that surprise newcomers:

- Field order carries no meaning, and a response will not echo the order you
  sent.
- There is no `Content-Type`. The body format follows from the command invoked,
  and is usually JSON.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Conventions and Terminology](#2-conventions-and-terminology)
3. [Message Format](#3-message-format)
4. [Framing](#4-framing)
5. [Line Endings](#5-line-endings)
6. [Header Fields](#6-header-fields)
7. [Message Body](#7-message-body)
8. [Method Lines](#8-method-lines)
9. [Status Codes](#9-status-codes)
10. [Connection Management](#10-connection-management)
11. [Command Control Fields](#11-command-control-fields)
12. [Escalation Profile](#12-escalation-profile)
13. [Replication Profile](#13-replication-profile)
14. [Security Considerations](#14-security-considerations)
15. [Known Defects](#15-known-defects)
16. [Divergences from HTTP/1.1](#16-divergences-from-http11)
- [Appendix A. Examples](#appendix-a-examples)
- [Appendix B. Conformance Requirements](#appendix-b-conformance-requirements)
- [Appendix C. Implementation Map](#appendix-c-implementation-map)

---

## 1. Introduction

### 1.1 Scope

BWP is a request/response protocol over TCP. The same message format serves
three roles:

| Profile | Port | Between | Section |
|---|---|---|---|
| Client | `-serverHost` (default `localhost:8888`), `-controlPort` | a client and a node | 3-11 |
| Escalation | `-commandPortPrivate` | two nodes, forwarding a client's command | 12 |
| Replication | `-nodeHost` | two nodes, replicating data | 13 |

Sections 3 through 8 apply to all three. Sections 9 through 11 are specific to
the client profile.

### 1.2 Design Intent

BWP borrows HTTP's shape so that a person can drive it with `netcat` and a
machine can drive it with a few lines of socket code. It borrows neither HTTP's
strictness nor its semantics. In particular:

- There is no protocol version, and therefore no version negotiation.
- There is no method registry. A method line is whatever the receiving plugin
  matches on.
- Status codes are freeform strings, not integers from a fixed set.
- Header field values carry backslash escapes, so a value may contain a newline.

### 1.3 What This Document Does Not Cover

- The MySQL protocol emulation, which is a separate protocol on a separate port.
- Plugin command semantics: the verbs and fields of Jobs, Cache, and
  Compression. See `docs/jobs.md` and `docs/cache.md`.
- The SQLite dialect accepted by the `Query` command.
- The consensus algorithm. Section 13 specifies the message format and the
  validation each message receives, not the state machine that drives it. See
  `docs/synchronization.md` for the algorithm.
- The internal design of any implementation. This document specifies what
  travels on the wire and what it means. Appendix C maps the protocol onto the
  Bedrock source for readers who need it, and is the only part of this document
  that names internal functions.
- The behaviour of any particular client library. Where this document says what a
  client must or should do, that obligation follows from what a node does with
  the bytes, not from what an existing client happens to do.

---

## 2. Conventions and Terminology

### 2.1 Requirement Keywords

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, RECOMMENDED, MAY, and OPTIONAL
are to be interpreted as described in RFC 2119.

These keywords describe what an implementation has to do to interoperate. Where
current behaviour is judged defective, the keyword describes the safe behaviour
and section 15 records the defect.

### 2.2 How to Read the Message Diagrams

Message diagrams show what travels on the wire. Unless a diagram says otherwise,
every line ends with CRLF and a blank line in the diagram is a real blank line
on the wire.

| Notation | Meaning |
|---|---|
| `Status` | literal text, exactly as shown |
| `<name>` | a value the sender supplies |
| `[ ... ]` | optional |
| `CRLF` | the two octets 0x0D 0x0A |
| `LF` | the single octet 0x0A |
| `CR` | the single octet 0x0D |
| `HTAB` | the single octet 0x09 |
| `NUL` | the single octet 0x00 |

Octet values are written in hex as `0xNN`.

### 2.3 Message Terms

* **Node** - a running `bedrock` process.
* **Client** - any process, remote or local, that connects to a command port or
  the control port.
* **Message** - one method line, its header fields, and its body.
* **Method line** - the first line of a message. In a request it names the
  command. In a response it carries the status.
* **Header field** - a `name: value` pair, referred to as a header throughout
  this document and in the Bedrock source.
* **Body** - the octets after the blank line. Called `content` in the Bedrock
  source.
* **Command** - a request that has been accepted by a node and bound to a plugin
  for execution.

### 2.4 Command Phases

A command executes in up to three phases. The phase names appear verbatim in
timeout status lines (section 9.2), so a client needs them to interpret an
error.

* **Read phase** - runs read-only. Many commands complete here and never write.
  A timeout reports `555 Timeout peeking command`.
* **Write phase** - may write to the database, and runs only on the leader. Its
  changes are committed at the end of the phase. A timeout reports
  `555 Timeout processing command`.
* **Post-commit phase** - runs only after the write phase committed
  successfully. A timeout reports `555 Timeout postProcessing command`, and is
  the one timeout that is not safe to retry (section 9.3).

### 2.5 Cluster Terms

Node state appears on the wire in three places, so the legal values are part of
the protocol:

- the `state` field of the `Status` command's response body;
- the reason phrase of the two health-check statuses (section 8.3); and
- the `State` field of `LOGIN` and `STATE` messages (section 13.4).

A node reports exactly one of these values. A receiver MUST accept all of them,
and MUST NOT assume the set is closed:

`SEARCHING`, `SYNCHRONIZING`, `WAITING`, `STANDINGUP`, `LEADING`,
`STANDINGDOWN`, `SUBSCRIBING`, `FOLLOWING`

What causes a node to move between these states is the consensus algorithm and
is out of scope here; see `docs/synchronization.md`.

Four states carry obligations stated elsewhere in this document, so they are
glossed here:

| State | Why it matters to the protocol |
|---|---|
| `LEADING` | The one node that commits writes. Writes sent elsewhere are escalated to it (section 12). |
| `FOLLOWING` | Replicates from the leader and serves reads. Accepts and escalates writes. |
| `LEADING` or `FOLLOWING` | The only two states in which the public command port is open (section 10.5). |
| `SYNCHRONIZING`, `STANDINGUP` | Gate which replication messages are legal (section 13.6). |

Other terms used by the replication and escalation profiles:

* **Peer** - another node named in `-peerList`.
* **Permafollower** - a node configured with priority 0. It never leads, is not
  counted when deciding whether a majority is present, and MUST NOT vote on
  transactions (section 13.9).
* **Commit count** - the number of transactions committed to a node's database.
  Monotonic. Carried as `CommitCount` between peers and `commitCount` to
  clients, and used throughout the protocol to compare how current two databases
  are.
* **Fork** - two nodes whose histories have diverged, detected by comparing the
  hash of a shared commit (section 13.8) and reported by the `FORKED` message.

---

## 3. Message Format

### 3.1 Shape

```
<method line>
<field name>: <field value>
<field name>: <field value>

<body>
```

Zero or more field lines. The blank line is always present, even when there are
no fields and no body. The body is optional.

### 3.2 Element Rules

| Element | Rules |
|---|---|
| method line | At least one octet. MUST NOT contain CR or LF. On output MUST NOT contain any octet below `0x20` other than `HTAB`, nor `0x7F`. |
| field name | At least one octet. MUST NOT contain `:`, CR, or LF. Compared case-insensitively. Surrounding spaces are stripped. |
| field value | Any octets except CR, LF, and NUL. Backslash escapes per section 6.3. Surrounding spaces are stripped before unescaping. |
| body | Any octets, including NUL. Length determined by section 4.3. |
| line ending | On output, CRLF. On input, any of four forms; see section 5. |

A field line whose first octet is whitespace is a continuation of the previous
field; see section 6.5.

### 3.3 Field Order Carries No Meaning

Fields carry meaning by name alone. A sender MUST NOT rely on field order, and
MUST NOT define semantics that depend on it.

A receiver MAY emit fields in any order, and is not required to preserve the
order in which they arrived. A message that is received and then forwarded, as
in section 12, may reach its next hop with its fields in a different order.

Each field name carries one value. Section 6.4 specifies how duplicate names on
the wire collapse to one.

### 3.4 Character Encoding

BWP is octet-oriented, not character-oriented. There is no charset negotiation
and no `Content-Type` field (section 7.3).

Field values SHOULD be UTF-8. The `\uXXXX` escape decodes to UTF-8 on input.

Field values MUST NOT contain NUL; see section 15.1.

---

## 4. Framing

A receiver reads a stream of octets and must decide where each message ends. BWP
has no length prefix, so framing derives from the blank line and the
`Content-Length` field.

### 4.1 Message Completeness

A receiver examining the front of its buffer gets one of two outcomes:

- **Incomplete.** Not enough octets have arrived. The receiver MUST retain the
  buffer unchanged and try again after reading more. Nothing is consumed.
- **Complete.** A message occupies the first N octets. Octets from N onward
  belong to the next message and MUST be retained.

A message is complete when the blank line has arrived and the body extent
determined by section 4.3 is fully present.

There is no in-band way to distinguish incomplete from malformed. A malformed
message that never completes is detected only by a timeout or by the peer
closing the connection.

### 4.2 The Blank Line

The field section ends at the first zero-length line.

A receiver MUST accept both `CRLF CRLF` and `LF LF` as the terminator. `LF LF`
is supported so that command-line tools work: pressing Enter twice in `netcat`
produces it.

These two are not the only sequences that terminate the fields, because the
terminator follows from the general line-ending rules of section 5 rather than
from matching a fixed byte sequence. `CR CR`, `LF CR LF CR`, and other
combinations also produce a zero-length line.

A sender SHOULD emit `CRLF CRLF`. This is not a MUST: an interactive sender such as a person typing into `netcat` MAY emits `LF LF`, and section 1.2 names that as a supported way to drive the protocol. Programmatic senders SHOULD emit `CRLF CRLF` because it is accepted everywhere without relying on section 5's tolerance.

### 4.3 Body Framing

Once the blank line is found, the body extent is determined by the first
applicable rule:

| Rule | Applies when | The body is |
|---|---|---|
| **Chunked** | `Transfer-Encoding: chunked` is present | the decoded chunks (section 7.1) |
| **Explicit length** | `Content-Length: N` is present | exactly N octets |
| **Read to end** | neither field is present | every remaining octet in the buffer |

Under **Explicit length**, if fewer than N octets follow the blank line the
message is incomplete. If more follow, the excess is the start of the next
message.

Under **Read to end** the whole buffer is consumed. This is not HTTP's "read
until connection close". The message boundary becomes the buffer boundary, which
depends on how TCP happened to segment the stream. See section 15.2.

`Content-Length: 0` is distinct from an absent `Content-Length`: it selects
**Explicit length** with an empty body.

### 4.4 Content-Length: Who Sets It, Who Reads It

This is a commonly misunderstood part of the protocol, because the two directions do different things.

**A receiver reads `Content-Length` and depends on it.** It is how the end of
the body is found under **Explicit length**. A wrong value silently mis-frames
every subsequent message on that connection.

**A sender always emits a `Content-Length` computed from the body it is
actually sending, and discards any value the caller asked for.** It is emitted
unconditionally, even for an empty body, so that there is no ambiguity. A
`Content-Length` on the wire is therefore always the true body length.

The obligations on a client follow from that:

| Requirement | Detail |
|---|---|
| MUST be exact | If `Content-Length` is present, it MUST equal the body length in octets. |
| MUST be present when reusing a connection | Omitting it selects **Read to end**, which would swallow whatever the client sends next. The same applies when pipelining. |
| MUST be a plain decimal integer | No sign, no whitespace, no units. See section 15.3. |
| MAY be omitted for a one-shot request | A single request on a connection the client will not reuse can rely on **Read to end**. This is what makes interactive `netcat` use possible. |

A client that builds messages through a library which computes `Content-Length`
should not set the field at all; whatever it sets will be replaced.

### 4.5 Pipelining

A sender MAY write several messages back to back. **Explicit length** framing
makes them separable, and a receiver loops until its buffer no longer holds a
complete message.

A client MUST NOT assume pipelined requests execute concurrently. On the client
profile, a node finishes sending each response before it reads the next request
on that connection, so responses arrive in request order and never interleave.
Pipelining reduces round trips; it does not increase parallelism.

---

## 5. Line Endings

### 5.1 Accepted Forms

Within the field section, a line ends at the first CR or LF. The terminator is
then consumed by the first matching rule:

| Order | Sequence | Octets consumed |
|---|---|---|
| 1 | `CR LF` | 2 |
| 2 | `LF CR` | 2 |
| 3 | `LF` | 1 |
| 4 | `CR` | 1 |

A receiver MUST apply these rules in order. A sender SHOULD use CRLF; see
section 4.2 for why this is not a MUST.

### 5.2 Mixing Within One Message

A receiver MUST accept different line endings on different lines of the same
message, for example CRLF on the method line, a bare CR after the first field,
and a bare LF after the second.

### 5.3 LF LF Is Two Endings, Not One

`LF LF` deliberately does not match rule 1 or rule 2. It is consumed as two
applications of rule 3. The first ends the preceding line; the second produces a
zero-length line, which terminates the fields.

The distinction matters only when reasoning about the rules above. On the wire,
`LF LF` terminates the fields, which is what an interactive sender needs.

### 5.4 Chunked Framing Requires CRLF

Section 5.1 does not extend into a chunked body. A receiver assumes a two-octet
terminator after the chunk-size line. A sender using `Transfer-Encoding: chunked`
MUST terminate the chunk-size line with CRLF. See section 15.5.

---

## 6. Header Fields

### 6.1 Case Insensitivity

Field names are compared case-insensitively; for example: `Content-Length`, `content-length`, and `CONTENT-LENGTH` are the same field.

A receiver MUST fold case. A sender SHOULD use the canonical spelling given in
this document. A sender that varies the case of one field across a single
message will still overwrite its own earlier value, since the spellings collapse
to one field.

Case folding is ASCII-oriented and locale-dependent. Field names SHOULD be
ASCII.

### 6.2 Field Name Restrictions

A field name MUST NOT contain `:`. A receiver splits the line at the first `:`,
so an embedded colon truncates the name and corrupts the value.

A field name MUST NOT contain CR or LF.

Field names are not escaped on output; the escape in section 6.3 applies to
values only. A sender is therefore responsible for encoding its own field names.

Bedrock does this where required. SQLite bound parameters are named `:id`,
`@id`, or `$id`, all of which would break the wire format, so the parameter name
is encoded before it becomes a field name: `:`, `@`, `$`, and `#` each become
`#XX`, where `XX` is the uppercase hex of the octet. Parameters travel as
`sql-param-<encoded-name>`, and Appendix A.4 shows the result.

Leading and trailing spaces are stripped from a field name.

### 6.3 Field Value Escaping

**On output**, four octets are escaped:

| Octet | Emitted as |
|---|---|
| CR | `\r` |
| LF | `\n` |
| HTAB | `\t` |
| `\` | `\\` |

**On input**, a wider set is decoded:

| Sequence | Decodes to |
|---|---|
| `\b` | 0x08 |
| `\f` | 0x0C |
| `\n` | LF |
| `\r` | CR |
| `\t` | HTAB |
| `\uXXXX` | the UTF-8 encoding of code point `XXXX` |
| `\` followed by any other octet | that octet |

The purpose is to let a field value carry a multi-line document on one physical
line. Throughout the Bedrock command set, a JSON object goes in a field, not in
the body.

The two sets are asymmetric: four escapes are produced, eight forms are
understood. A sender that escapes according to the output table is safe, because
that table doubles every literal backslash. A sender that hand-rolls its
escaping MUST still escape `\` as `\\`, or a value containing the five
characters `\u0041` will arrive as the single character `A`.

Leading and trailing spaces are stripped from a value before unescaping. A value
that must preserve surrounding whitespace MUST escape it as `\t`, or encode the
value some other way.

### 6.4 Duplicate Field Names

When a field name repeats on the wire, the later value replaces the earlier one.
Last occurrence wins.

`Set-Cookie` is the sole exception. Repeated `Set-Cookie` lines all survive: a
message received with three `Set-Cookie` lines is re-emitted with three. One
line per cookie is used rather than RFC 2109 section 4.2.2's comma-delimited
form, because Firefox did not honour that form.

The octet 0xFF is reserved as the separator that keeps multiple `Set-Cookie`
values distinct between receipt and re-emission. A `Set-Cookie` value MUST NOT
contain 0xFF, or it will be re-emitted as two cookies.

### 6.5 Continuation Lines

A line beginning with whitespace is a continuation. Its full content, including
the leading whitespace, is appended to the most recently parsed field value.

If no field has been parsed yet, the continuation appends to the method line
instead. This differs from HTTP, where folding before the first field is
malformed.

A continuation is appended without unescaping, unlike a normal field value. A
sender SHOULD NOT use continuation lines at all: section 6.3 escaping serves the
same purpose and round-trips correctly.

### 6.6 Fields a Sender Rewrites

Three fields do not appear on the wire as the sender set them. Whatever value is
supplied for one of these, a different value may be sent:

| Field | What is actually sent |
|---|---|
| `Set-Cookie` | one line per cookie, split on the 0xFF separator (section 6.4) |
| `Content-Length` | the real body length, replacing any supplied value (section 4.4) |
| `Content-Encoding: gzip` | present only if the body was in fact compressed (section 7.2) |

For every other field, the value sent is the value supplied, subject to the
escaping in section 6.3.

---

## 7. Message Body

### 7.1 Chunked Transfer Encoding

A receiver MUST accept `Transfer-Encoding: chunked`. A sender SHOULD NOT emit
it: Bedrock never produces chunked output, so chunked input reaches a Bedrock
parser only from third-party HTTP servers contacted by Bedrock's outbound HTTPS
client.

```
<chunk size in hex>[;<extension>]
<chunk data>
...
0[;<extension>]
[ <trailer field> ]

```

Rules a receiver applies:

1. The chunk-size line is truncated at the first `;`. Everything after it is
   discarded without inspection.
2. The remainder MUST be 1 to 8 hex digits. A line that is not is not an error:
   it is reparsed as a trailer field. See section 15.4.
3. Chunk data begins two octets after the chunk-size line (section 5.4) and runs
   for the stated length. If the buffer ends first, the message is incomplete.
4. A chunk size of `0` ends the chunk sequence.
5. A blank line MUST follow the last chunk and its trailers. Without it the
   message is incomplete.

**Field rewriting.** On success a receiver mutates the field set: it removes
`Transfer-Encoding` and inserts a `Content-Length` computed from the decoded
body. Either field tells a caller the connection may carry more data, so
removing one requires adding the other.

A consequence is that chunked framing never survives a hop. A receiver that
forwards or re-emits a message it received chunked sends it with
`Content-Length` instead, and `Transfer-Encoding` is gone.

**Trailers.** Fields after the last chunk are merged into the same field set as
the headers, using the same last-wins rule. A trailer therefore MAY overwrite a
field sent before the body. A sender MUST NOT rely on a field surviving a
same-named trailer.

### 7.2 Content-Encoding: gzip

A sender requests compression by setting `Content-Encoding: gzip` before the
message is serialized. On the wire the outcome is one of two things:

- the field is present and the body is gzip-compressed; or
- the field is absent and the body is uncompressed, because compression was
  skipped for an empty body or produced no output.

There is no error signal for the second case. A receiver MUST NOT assume gzip
was honoured merely because it was requested, and MUST decide from the field on
the message in front of it.

A receiver does not decompress as part of parsing. Decompression is the
application's responsibility.

`Content-Encoding` values other than `gzip` are ordinary fields with no effect.

### 7.3 There Is No Content-Type

BWP has no `Content-Type` negotiation, and Bedrock never sets the field. A
client determines the body format from the command it invoked.

In practice:

- Most commands return a JSON object.
- The `Query` command is the notable exception. By default it returns a
  human-readable table, and returns JSON only when the request sets
  `Format: json`.

A client that must handle an unfamiliar command SHOULD attempt a JSON parse and
treat failure as "this body is not JSON" rather than as an error. The correct handling of a body like this is undefined.

### 7.4 Body Octet Transparency

The body is not escaped, not transformed, and not required to be text. It is
carried verbatim and its length is authoritative. A body MAY contain NUL, unlike
a field value; see section 15.1.

### 7.5 Nested Messages

A body MAY itself contain whole BWP messages, concatenated with no separator and
no count prefix. The replication profile uses this: a `SYNCHRONIZE_RESPONSE`
body is a run of `COMMIT` messages. See section 13.5 and Appendix A.7.

Nesting works only because of **Explicit length** framing (section 4.3): each
nested message carries its own `Content-Length`, so the run is self-delimiting.
A nested message therefore MUST carry `Content-Length`.

---

## 8. Method Lines

### 8.1 Request Method Lines

```
<verb>[ <argument>]
```

Three forms occur:

#### 1: Bare verb
The common case, matched case-insensitively: `Status`, `Ping`, `Query`, `Detach`, `BeginBackup`.

#### 2: Verb with inline argument
The `Query` command accepts SQL in the method line as shorthand for a `query` field:

```
Query: SELECT 1 AS foo;
```

The colon is required; the verb is case-insensitive.

#### 3: Full HTTP request line
Two health checks match a complete HTTP/1.1 request line, so that HAProxy, which can only emit real HTTP for liveness checks, can drive them:

```
GET /status/isFollower HTTP/1.1
GET /status/handlingCommands HTTP/1.1
```

These are matched as exact strings, not parsed as HTTP. Their responses report
node state and are the exception described in section 8.3.

A generic `<verb> <uri>` split is available to a receiver, but most commands do
not use it and match the whole method line instead. A client MUST send the exact
method line a command expects.

### 8.2 Method Line Restrictions on Output

A sender MUST NOT emit a method line containing any octet below `0x20` other than
`HTAB`, or `0x7F`. Bedrock rejects such a line rather than emitting it.

This prevents response splitting through a crafted status line; see section 14.1.

Note that the method line is validated, while field values are escaped (section 6.3), and field names are neither. The three are not interchangeable.

### 8.3 Response Method Lines

```
<status code>[ <reason phrase>]
```

A Bedrock response normally carries no protocol token:

```
200 OK
```

A client MUST extract the status by parsing the leading integer of the whole
line. It MUST NOT expect an `HTTP/1.1` prefix, and MUST NOT rely on a fixed
number of space-separated tokens: reason phrases vary in length and some contain
`=` and `.`, for example `500 Not Following. State=SEARCHING`.

The two health checks of section 8.1 are the exception and do emit a protocol
token, because HAProxy requires one:

```
HTTP/1.1 200 Following
HTTP/1.1 500 Not Following. State=SEARCHING
```

### 8.4 The Reason Phrase Is Semantic

Unlike HTTP, the reason phrase is not decorative. Two mechanisms read it.

**Log severity selection.** When a command fails, a node picks a log level by
substring-matching the status line:

| Status line contains | Log level |
|---|---|
| `_ALERT_` | ALERT |
| `_WARN_` | WARN |
| `_HMMM_` | HMMM |
| begins `50` | ALERT |
| otherwise | INFO |

**Retry safety.** `555 Timeout postProcessing command` must be distinguished
from every other `555 Timeout` status by its words alone, because only that one
runs after a commit. See section 9.3.

A server implementation MUST therefore treat the reason phrase as part of the
protocol, and MUST NOT reword an existing status line.

---

## 9. Status Codes

### 9.1 Model

A status code is the leading integer of the response method line. Codes are not
drawn from a registry; any string may appear. Codes broadly follow HTTP's classes:

- `2xx` success
- `4xx` caller error
- `5xx` server error

Two codes, `430` and `555`, are outside HTTP's registry.

### 9.2 Registry

| Code | Reason phrase | Returned when |
|---|---|---|
| `200` | `OK` | default when a command set no status |
| `202` | `Successfully queued` | fire-and-forget accepted, before execution |
| `203` | `DETACHING` | `Detach` control command accepted |
| `204` | `ATTACHING` | `Attach` control command accepted |
| `400` | `Unique Constraints Violation` | a uniqueness constraint failed during commit |
| `400` | `Already detached` | `Detach` when already detached |
| `401` | `Unauthorized` | control command from a non-localhost source |
| `401` | `Attaching prevented by ...` | a plugin or node state blocks `Attach` |
| `402` | `Missing query` | `Query` with empty or oversized SQL |
| `402` | `Bad query` | SQL failed to prepare, or the read failed |
| `430` | `Unrecognized command` | no plugin claimed the method line |
| `500` | `Refused` | crash-suppressed command (section 14.6), or an unsupported outbound request |
| `500` | `Unhandled Exception` | an unexpected failure in a command phase |
| `500` | `Server Shutting Down` | non-control command on the control port during shutdown |
| `500` | `Leader stopped leading` | leadership lost mid-commit |
| `502` | `Query Missing Semicolon` | `Query` SQL did not end in `;` |
| `502` | `Query aborted` | `UPDATE` or `DELETE` with no `WHERE` and no `nowhere` field |
| `502` | `Query failed` | the write failed |
| `555` | `Timeout peeking command` | deadline hit during the read phase |
| `555` | `Timeout processing command` | deadline hit during the write phase |
| `555` | `Timeout postProcessing command` | deadline hit during the post-commit phase |

This table is not closed. A plugin may introduce a new status line at any time,
so a client MUST handle unknown codes by class.

### 9.3 Retry Guidance

A client MUST NOT decide retry safety from the status class alone. The rules
below follow from what a node did before it answered.

| Situation | Retry? | Why |
|---|---|---|
| Connection failed before the request was fully sent | yes | the command never reached a node |
| `555 Timeout peeking command` | yes | the transaction was rolled back, so nothing was applied |
| `555 Timeout processing command` | yes | the transaction was rolled back, so nothing was applied |
| `555 Timeout postProcessing command` | **no** | the post-commit phase runs only after the command committed and returned success. Retrying re-applies committed work. |
| `500 Refused` | **no** | the command matched one that previously crashed a node (section 14.6). Retrying spreads the crash. |
| any other error, request fully sent | only if the command is idempotent by its own semantics | the outcome is unknown |

The post-commit case is the subtle one, and it is why section 8.4 insists the
reason phrase is load-bearing. All three timeout statuses share the code `555`;
only the words distinguish a rolled-back timeout from a post-commit one.

A client MAY attach its own bookkeeping fields to a request, such as a flag
recording that a command is safe to replay. A node ignores fields it does not
recognise, so such a field is a client-side convention and not part of this
protocol.

---

## 10. Connection Management

### 10.1 Default: Persistent

A connection is persistent by default. A node reads a message, executes it,
sends the response, and then reads the next message on the same connection,
until the connection closes. One request is in flight at a time (section 4.5).

### 10.2 The Connection Field

```
Connection: close
Connection: forget
```

Matched case-insensitively. Any other value, including absence, means persistent.

**`Connection: close`.** A node echoes `Connection: close` on the response and
then shuts the connection down, in case the caller ignores the field. A client
MUST NOT send another request on that connection.

A node MAY set `Connection: close` on a response the client did not ask to
close, when it is shutting down or has blocked its command port. A client MUST
honour it.

**`Connection: forget`.** Fire and forget. A node:

1. Sends `202 Successfully queued` with `Connection: close` immediately, before
   executing anything.
2. Closes the connection.
3. Discards the command's real response.
4. Raises the default timeout from 110 seconds to 1 hour.

A client MUST NOT interpret `202` as evidence that the command succeeded. It
attests queuing only. If the node is shutting down, the command is discarded
after the `202` has already been sent.

### 10.3 Timeouts

Two independent deadlines apply.

| Deadline | Field | Default |
|---|---|---|
| whole command | `timeout` | 110 000 ms, chosen so a client can use a 120 second socket timeout |
| write phase | `processTimeout` | 5 000 ms |

Both are in milliseconds on the wire.

A node MUST abort a command whose deadline passes and respond `555`. A client
SHOULD set `timeout` below its own socket timeout, so that it receives a `555`
it can reason about rather than timing out blind.

### 10.4 Client Disconnect

A node watches the client connection while a command runs, and attempts to abort
the command if the client disconnects.

Abort is best-effort. A client that abandons a request MUST NOT assume the
command was cancelled, and MUST treat the outcome as unknown. In particular, a
write may still have committed.

### 10.5 Ports

Four listening sockets. All four speak the message format of sections 3 to 8.

| Argument | Default | Role | Restriction |
|---|---|---|---|
| `-serverHost` | `localhost:8888` | public command port | open only while `LEADING` or `FOLLOWING` |
| `-commandPortPrivate` | none | escalation from peers | requests treated as localhost |
| `-controlPort` | none | control commands | control commands require an empty `_source` |
| `-nodeHost` | none | replication | `LOGIN` must name a configured peer |

A node may stop accepting client traffic at any time. When it does, the public
command port closes and any open connection is told `Connection: close`. It
reopens without the node restarting. Reasons include leaving `LEADING` or
`FOLLOWING`, falling too far behind the leader to serve current data, resource
exhaustion, backup, and detach.

A client MUST tolerate the command port disappearing and reappearing, and MUST
NOT treat either as an error. The thresholds that trigger it are a matter for
the node's configuration, not for this protocol.

---

## 11. Command Control Fields

These fields modify how a node executes a command. They are part of the
envelope, not of any command's own parameters.

### 11.1 Request Fields

| Field | Type | Meaning |
|---|---|---|
| `timeout` | ms | command deadline (section 10.3) |
| `processTimeout` | ms | write-phase deadline |
| `writeConsistency` | `0`, `1`, or `2` | `0` ASYNC, `1` ONE, `2` QUORUM. An invalid value falls back to ASYNC and is logged. |
| `commitCount` | integer | hold the command until this node's database has reached this commit |
| `mockRequest` | present | execute the command but discard any writes |
| `requestID` | string | log correlation. A node generates six random alphanumerics if absent. |
| `logParam` | string | an extra value to attach to log lines |
| `priority` | integer | scheduling hint |
| `commandExecuteTime` | microseconds since epoch | **deprecated.** Any future value forces fire-and-forget. |
| `Connection` | `close` or `forget` | section 10.2 |
| `Content-Length` | integer | section 4.4 |
| `Content-Encoding` | `gzip` | section 7.2 |

Unrecognised fields are ignored, not rejected.

`writeConsistency` is the client's consistency lever. It selects how many other
nodes must confirm a write before the leader commits it:

| Value | Name | The leader commits once |
|---|---|---|
| `0` | ASYNC | immediately, without waiting for any other node |
| `1` | ONE | one other node has confirmed |
| `2` | QUORUM | a majority of the nodes that count toward a majority have confirmed |

Lower values are faster and less durable. ASYNC lets the leader run ahead of the
cluster, so a commit can be lost outright if the leader dies before any other
node has it. `docs/synchronization.md` gives the guidance: ASYNC for a comment on
a report, QUORUM for reimbursing an expense report.

A node MAY override the requested consistency (method lines listed in
`-synchronousCommands` are forced to QUORUM).

### 11.2 Response Fields

| Field | Always present | Meaning |
|---|---|---|
| `commitCount` | yes | the responding node's commit count at reply time |
| `nodeName` | yes | `-nodeName` of the responding node |
| `Content-Length` | yes | section 4.4 |
| `Connection` | conditional | `close`, when requested or when shutting down |
| `error` | conditional | human-readable error text, typically from SQLite |
| `exceptionSource` | conditional | file and line of the failure, only with `-extraExceptionLogging` |
| timing fields | yes | per-phase durations and counts |

A client SHOULD record `commitCount` from a write response and send it back on
subsequent requests, so that a read served by a different node cannot observe
older data. A node given a `commitCount` higher than its own delays its response
until it has caught up. `docs/synchronization.md` explains the mechanism.

### 11.3 Reserved Field Names

A client MUST NOT set the following. A node sets, overwrites, or strips them.

| Field | Set by | Notes |
|---|---|---|
| `_source` | receiving node | peer IP address. Omitted when the peer is `127.0.0.1` or arrived on the private command port. |
| `plugin` | receiving node | which plugin claimed a plugin-port request |
| `ID` | escalating node | command identifier, used to route the reply |
| `httpsRequests` | escalating node | serialized in-flight outbound HTTPS transactions; stripped before the command sees it |
| `serializedData` | escalating node | serialized plugin state; likewise stripped |

`_source` inverts the usual convention: absence means trusted. Control commands
are gated on `_source` being empty. See section 14.3.

---

## 12. Escalation Profile

### 12.1 Purpose

Any node accepts any command. A node that cannot execute a command locally,
typically a follower receiving a write, forwards it to a node that can. The
forwarded message is the client's message, in the same format, on the private
command port. The client protocol is the internal RPC protocol.

### 12.2 Transformation

The escalating node takes the original request and modifies it:

1. **`timeout` is rewritten** to the remaining budget, in wire milliseconds. An
   escalating node MUST do this. Forwarding the original timeout would give the
   receiver a full fresh budget and allow unbounded total latency across a chain
   of escalations.
2. **`ID` is set** to the command's identifier, of the form
   `<nodeName>#<counter>`.
3. **`httpsRequests` is set**, if the command has outbound HTTPS transactions in
   flight. Each transaction's request and response are themselves BWP messages,
   serialized and base64-encoded.
4. **`serializedData` is set**, if the plugin has state to carry.

Everything else, including `requestID`, `writeConsistency`, and the body, is
forwarded verbatim.

### 12.3 Reception

The receiving node parses the message exactly as a client message, then:

- Treats the source as localhost, so `_source` is not set. This is what allows
  an escalated control command to pass the section 11.3 check.
- Strips `httpsRequests` and `serializedData` before constructing the command,
  and applies them to the command.
- Maps a failure to reconstruct the command to a response rather than a
  disconnect. A recognised failure yields its own status line; anything else
  yields `500 UNKNOWN ERROR`.

### 12.4 Response Handling

The escalating node reads octets until a complete message is available, then
replaces the command's response wholesale, status line, fields, and body, and
marks the command complete. It sends exactly one command per connection
acquisition, so it expects exactly one response.

### 12.5 Failure and Retry

The escalation path distinguishes two failure points, because their safety
properties differ:

- **Before the request is fully sent**, no error response is set, so the command
  can be requeued and retried, possibly against a new leader.
- **After the request is fully sent**, the outcome is unknown, so an error
  response is set.

A node retries for up to five seconds while looking for a usable peer.

---

## 13. Replication Profile

### 13.1 Scope

Nodes named in `-peerList` connect to each other on `-nodeHost` and exchange
messages in the same format. This section specifies those messages: their
fields, their required fields, and the validation applied. It does not specify
the consensus algorithm; see `docs/synchronization.md`. Section 2.5 defines the
states and roles referred to below.

Framing is as in section 4.

### 13.2 Connection Establishment

1. Both nodes attempt to connect to each other. Simultaneous connection is
   expected.
2. On connect, a node sends `LOGIN` immediately, then `PING`.
3. An accepting node MUST receive `LOGIN` as the first message. Any other first
   message is an error and the connection is closed.
4. The `Name` field of `LOGIN` MUST match a configured peer. An unknown name is
   rejected as unauthenticated.
5. A connection that sends nothing for five seconds is closed.
6. If a peer is already connected, the second connection is refused rather than
   replacing the first, because message order matters and swapping connections
   would break an in-flight sequence.

Peer authentication is name-based only. There is no shared secret, no token, and
no in-protocol TLS. See section 14.4.

### 13.3 Universal Peer Fields

Every message except `PING` and `PONG` MUST carry `CommitCount` and `Hash`,
which describe the sender's database. A sender adds them automatically if the
message did not set them.

| Field | Required | Meaning |
|---|---|---|
| `CommitCount` | yes | the sender's commit count |
| `Hash` | yes | hash of the sender's commit at `CommitCount` |
| `commandAddress` | always sent | the sender's private command port, for escalation routing |

A receiver MUST reject a message missing `CommitCount` or `Hash`. Rejection has
a defined protocol effect; see section 13.6.

A receiver MUST accept `PING` and `PONG` without these fields. They are handled
before validation, and also before the forked-peer check, so that liveness survives a fork.

### 13.4 Message Registry

Direction is relative to the sender's role.

| Method line | Direction | Required fields | Purpose |
|---|---|---|---|
| `LOGIN` | any to any | `Name`, `Priority`, `State`, `Version`, plus `Permafollower` | first message; announces identity, priority, state, version |
| `PING` | any to any | `Timestamp` | liveness probe |
| `PONG` | any to any | `Timestamp`, echoed | liveness reply; the sender computes round-trip time from it |
| `STATE` | any to all | `State`, `Priority`, `StateChangeCount` | broadcast on state change, priority change, and each commit |
| `STANDUP_RESPONSE` | any to a candidate | `Response`, either `approve` or `deny`; `StateChangeCount`; optionally `Reason` | vote on a standup attempt |
| `SYNCHRONIZE` | any to a peer | none beyond 13.3 | request missing commits |
| `SYNCHRONIZE_RESPONSE` | peer to requester | `NumCommits`; optionally `ShuttingDown`, `hashMismatchValue`, `hashMismatchNumber` | body carries up to 100 `COMMIT` messages |
| `SUBSCRIBE` | a `WAITING` node to the leader | none beyond 13.3 | request to begin `FOLLOWING` |
| `SUBSCRIPTION_APPROVED` | leader to follower | `NumCommits` | body carries all remaining `COMMIT` messages |
| `BEGIN_TRANSACTION` | leader to followers | `ID`, `NewCount`, `NewHash`, `leaderSendTime`, `dbCountAtStart`; body is the query | phase one of the distributed commit |
| `APPROVE_TRANSACTION` | follower to leader | `ID`, `NewCount`, `NewHash`; optionally `AsyncNotification` | the follower prepared successfully |
| `DENY_TRANSACTION` | follower to leader | `ID`, `NewCount`, `NewHash` | the follower failed to prepare |
| `COMMIT_TRANSACTION` | leader to followers | `ID`, `NewCount`, `NewHash` | phase two: commit |
| `ROLLBACK_TRANSACTION` | leader to followers | `ID` | phase two: abandon |
| `FORKED` | any to a peer | none beyond 13.3 | "your history disagrees with mine" |
| `RECONNECT` | any to a peer | `Reason` | sent immediately before closing on a protocol error |
| `COMMIT` | nested only | `CommitIndex`, `Hash`; body is the query | one replicated transaction; never sent standalone |

An unrecognised method line is logged and ignored, not treated as an error.

### 13.5 Nested COMMIT Messages

`SYNCHRONIZE_RESPONSE` and `SUBSCRIPTION_APPROVED` carry commits in their body
as concatenated `COMMIT` messages. A sender does:

```
set NumCommits on the outer message to the number of commits being sent
for each commit to send:
    build a COMMIT message with
        CommitIndex = the commit's sequence number
        Hash        = the commit's hash
        body        = the commit's query
    append its serialized form to the outer message's body
```

A receiver loops over the body, parsing one message at a time until no complete
message remains. For each it MUST verify:

1. The method line is `COMMIT`.
2. `CommitIndex` is present and non-negative.
3. `Hash` is present.
4. `CommitIndex` equals the local commit count plus one. Commits apply strictly
   in order.
5. After applying the query, the recomputed hash equals `Hash`. On mismatch the
   transaction is rolled back and the message rejected.

The count MUST reconcile: `NumCommits` decrements per nested message, and a
nonzero remainder at the end of the body is an error.

An empty `COMMIT` body is not fatal but is alerted.

`SYNCHRONIZE_RESPONSE` sends at most 100 commits per message, and the requester
loops. `SUBSCRIPTION_APPROVED` sends everything remaining, with the gathering
query bounded at half the 30 second peer receive timeout.

Appendix A.7 shows the resulting message.

### 13.6 Errors Are Connection-Fatal

Section 9's status codes do not exist in this profile. There are no responses in
the request/response sense; a validation failure aborts the connection. The
sender emits a `RECONNECT` message carrying a `Reason` field, then closes.

A receiver MUST treat `RECONNECT` as notice that the sender is closing. The
`Reason` field is diagnostic only.

State-dependent messages are rejected when they arrive in the wrong state:
`SYNCHRONIZE_RESPONSE` at a node that is not `SYNCHRONIZING`, `SUBSCRIBE` at a
node that is not `LEADING`, and any message other than `LOGIN` before login.

Two cases do not abort the connection, because arriving in the wrong state is
expected:

- `BEGIN_TRANSACTION`, `COMMIT_TRANSACTION`, and `ROLLBACK_TRANSACTION` at a node
  that is not `FOLLOWING` are logged and dropped, since a stream of these may
  still be in flight as a node leaves `FOLLOWING`.
- `STANDUP_RESPONSE` at a node that is not `STANDINGUP` is dropped as a late
  message.

### 13.7 Transaction Body Compression

The body of `BEGIN_TRANSACTION` and of a nested `COMMIT` is the SQL query,
optionally zstd-compressed. A receiver attempts decompression unconditionally,
which is safe because decompression is a no-op on input that lacks a zstd frame
header.

A receiver MUST tolerate both forms. Compression is detected from the frame
header, not from a protocol field, so no `Content-Encoding` is involved.

### 13.8 Fork Detection

`Hash` on every message (section 13.3) makes divergent history detectable at any
time.

- At `LOGIN`, if the peer's `CommitCount` is at or below ours and the hashes for
  that commit differ, we send `FORKED` and mark the peer forked.
- During `SYNCHRONIZE`, a hash mismatch is reported in the response as
  `hashMismatchValue` and `hashMismatchNumber` rather than by disconnecting, so
  that the requester learns which commit diverged.
- On receiving `FORKED`, a node believes the sender unconditionally.

A forked peer is not disconnected. It stays connected, but every message except
`PING` and `PONG` is ignored in both directions.

### 13.9 Async Approval Suppression

`BEGIN_TRANSACTION` with an `ID` of the form `ASYNC_<digits>` tells followers the
leader will not wait for approval. Followers still send roughly one approval in
ten, so the leader's view of follower progress stays current. Such a message
carries `AsyncNotification: true`.

A leader MUST treat `APPROVE_TRANSACTION` with `AsyncNotification` as a progress
report, and MUST NOT count it toward the confirmations required by the
transaction's `writeConsistency` (section 11.1).

A permafollower MUST NOT send `APPROVE_TRANSACTION` or `DENY_TRANSACTION` at
all. A leader rejects one that arrives from a permafollower.

### 13.10 Timing Constants

These are the values Bedrock uses. They are tuning choices rather than protocol
requirements, and are listed because an implementation on the other end of the
connection has to survive them.

| Constant | Value |
|---|---|
| peer receive timeout | 30 s |
| `PING` trigger | 25 s since last receive |
| minimum `PING` interval | 1 s |
| unauthenticated connection idle limit | 5 s |
| escalation peer search window | 5 s |
| minimum approval frequency for async transactions | every 10th commit |
| commits per `SYNCHRONIZE_RESPONSE` | 100 |

State timeouts add up to 5 seconds of jitter, to avoid synchronised retries
across the cluster.

---

## 14. Security Considerations

### 14.1 Response Splitting via the Method Line

A status line assembled from untrusted input could otherwise inject CRLF and
forge fields or a second response. Section 8.2 requires a sender to reject any
control octet in the method line rather than escape it.

An implementation MUST validate the method line on output.

### 14.2 Field Injection via Field Names

Field values are escaped, so CRLF in a value cannot forge a field. Field names
are not escaped (section 6.2). An implementation that builds a field name from
untrusted input MUST encode it. The bound-parameter encoding described in
section 6.2 is the pattern to copy.

### 14.3 The `_source` Convention

Control commands are authorised by `_source` being empty. Empty means the
request arrived from `127.0.0.1` or from the private command port.

Because a node only sets `_source` for non-localhost peers, a client connecting
over loopback can supply its own `_source`. This fails closed: a supplied value
is non-empty, so the control check rejects the command.

An implementation MUST NOT invert the test to allow-list specific `_source`
values, which would make the field forgeable from loopback.

Trust here rests on network reachability. The private command port grants
localhost-equivalent authority to anything that can reach it, so it MUST NOT be
exposed beyond the cluster.

### 14.4 Peer Authentication

Replication authenticates a peer by matching the `LOGIN` `Name` against
`-peerList` (section 13.2). There is no secret, no signature, and no in-protocol
TLS. Anything that can reach `-nodeHost` and guess a configured node name can
attempt to join the cluster.

The fork check in section 13.8 limits, but does not prevent, damage: a peer
whose history matches is accepted, and a peer whose history does not is ignored
rather than rejected. Deployments MUST confine `-nodeHost` to a trusted network.

Bedrock does use TLS for outbound HTTPS requests. Neither the client listener
nor the replication listener uses it.

### 14.5 Denial of Service

**Unbounded field accumulation.** The protocol sets no limit on the number of
field lines, the length of a field value, or the total size of the field
section. A peer that sends field lines indefinitely grows a receiver's buffer
without bound. Deployments MUST rely on limits outside the protocol.

**Body size.** Also unbounded by the protocol. Bedrock enforces plugin-level
limits, 1 MiB for a query and for a blob, but only after parsing. Their purpose
is to prevent silent truncation by the storage engine, not to bound memory.

**Connection cost.** A node commits resources per open connection. Bedrock
detects exhaustion and blocks its command port, degrading rather than failing,
which a client sees as the port disappearing (section 10.5).

### 14.6 Crash-Inducing Command Suppression

A command whose method line and identifying field values match one that
previously crashed a node is refused with `500 Refused` rather than executed.
The set of fields compared is chosen per plugin.

A client MUST treat `500 Refused` as non-retryable. Retrying propagates the
crash to another node.

---

## 15. Known Defects

Behaviour in this section is present in Bedrock and is documented so that it is
not mistaken for design. It is not normative. An implementation SHOULD NOT
depend on it, and a fix would not be a protocol change.

Every entry here is visible on the wire. Purely internal shortcomings are out of
scope for this document.

### 15.1 NUL Truncates a Field Value

Escaping and unescaping operate on NUL-terminated strings. A field value
containing an embedded NUL is silently truncated at that octet, on both send and
receive. No error is raised; the value simply arrives short.

This is why bound parameters base64-encode their text and blob payloads rather
than placing them in a field directly.

For implementers: a field value MUST NOT contain NUL. Encode binary values, or
put them in the body, which is NUL-safe (section 7.4).

### 15.2 Absent Content-Length Consumes the Whole Buffer

**Read to end** framing (section 4.3) makes the message boundary depend on TCP
segmentation. If a peer pipelines a second message behind a length-less first
message, and both arrive in one read, the second message becomes the first
one's body.

This is harmless for the interactive use the behaviour exists for, and dangerous
for anything programmatic.

For implementers: always send `Content-Length` (section 4.4).

### 15.3 Malformed Content-Length Is Not Handled

Section 4.4 requires `Content-Length` to be a plain decimal integer. A value
that is not is not rejected with a status code; it is not validated at all.

- A **non-numeric** value drops the connection. No response is sent, so a client
  sees the connection close mid-request rather than an error it can act on. On a
  replication connection the failure bypasses the `RECONNECT` handling of
  section 13.6, so the peer gets no `Reason`.
- A **negative** value is accepted as a length. It then passes the check for
  whether enough octets have arrived, because that check compares a positive
  count of available octets against a negative count of required ones, and the
  message proceeds with a nonsensical body length.

For implementers: reject a `Content-Length` that is not a sequence of decimal
digits, before using it, if you write an independent receiver.

### 15.4 Invalid Chunk Size Truncates Silently

Section 7.1 requires a chunk-size line to be 1 to 8 hex digits, and reinterprets
a line that is not as a trailer field rather than rejecting it.
The result is a message that parses successfully with a short body and no
indication of loss. Appendix B.9 states the current behaviour and notes that a
corrected implementation may reject instead.

### 15.5 Chunk Data Offset Assumes CRLF

Chunk data is taken to start two octets after the chunk-size line. If a sender
terminated that line with a single CR or LF, the offset overshoots by one octet,
the first octet of chunk data is lost, and all subsequent framing shifts.

The general line-ending tolerance of section 5.1 therefore does not hold inside
a chunked body.

### 15.6 Escape Asymmetry

Four escapes are produced and eight forms are understood (section 6.3). A sender
that escapes per the output table is safe, because that table doubles every
literal backslash. A sender that hand-rolls its escaping and omits that doubling
will corrupt any value containing a backslash sequence the decoder recognises.

### 15.7 The Shipped Response Parser Rejects the Shipped Response Format

Bedrock provides a response-line parser that expects
`<protocol> <code> <reason>`, a shape Bedrock's own responses do not use
(section 8.3). Given `200 OK` it reports failure and produces nonsense. It
remains useful only for real HTTP responses from the outbound HTTPS client, and
MUST NOT be used on a Bedrock response.

### 15.8 Continuation Lines Are Not Unescaped

Section 6.5. A continuation line is appended raw while a normal field value is
unescaped, so escapes in a continuation survive as literal backslash sequences.

---

## 16. Divergences from HTTP/1.1

A summary for readers who know HTTP.

| Aspect | HTTP/1.1 | BWP | Section |
|---|---|---|---|
| Version token | required on both lines | absent, except two health checks | 8.1, 8.3 |
| Line ending | CRLF only | CRLF, `LF CR`, CR, or LF, mixable | 5 |
| Field terminator | `CRLF CRLF` | also `LF LF` | 4.2 |
| Field value escaping | none | backslash escapes, `\uXXXX` decoded | 6.3 |
| Field name charset | token, no `:` | any octet except `:`, CR, LF; unescaped on output | 6.2 |
| Duplicate fields | comma-combined | last wins, except `Set-Cookie` | 6.4 |
| Field order | preserved | not preserved | 3.3 |
| Continuation lines | obsolete; illegal before the first field | supported, and fold into the method line | 6.5 |
| No `Content-Length` | read until close | read to end of buffer | 4.3, 15.2 |
| `Content-Length` on output | as supplied | always regenerated | 4.4 |
| Chunked output | supported | never emitted | 7.1 |
| Chunked input | `Transfer-Encoding` preserved | rewritten to `Content-Length` | 7.1 |
| Trailers | separate from fields | merged, may overwrite fields | 7.1 |
| `Content-Type` | negotiated | absent; format follows from the command | 7.3 |
| Status codes | registered integers | freeform strings, plus `430` and `555` | 9 |
| Reason phrase | decorative | semantic: drives log level and retry safety | 8.4 |
| `Connection` | `keep-alive` or `close` | `close` or `forget`; `forget` responds before executing | 10.2 |
| Pipelining | concurrent permitted | accepted, serialized | 4.5 |

---

## Appendix A. Examples

Two conventions, both of which matter if you recount the `Content-Length`
values:

- **The method line and every field line end with CRLF**, unless a note says
  otherwise. The blank line that ends the fields is also CRLF.
- **Line endings inside a body are whatever the command produced**, because a
  body is carried verbatim (section 7.4). In these examples that means: a JSON
  body has no line endings and no trailing newline; a body rendered as text, as
  in A.3 and A.6, uses LF and ends with one.

Every `Content-Length` below is the exact octet count of the body shown, under
those conventions.

### A.1 Minimal Request and Response

A request as a person would type it into `netcat`. Both lines end with a bare
LF, and there is no `Content-Length`, which is permitted for one-shot
interactive use (section 4.4):

```
Status

```

The response. `Content-Length`, `commitCount`, and `nodeName` are always
present. Do not read anything into the field order; see section 3.3.

```
200 OK
Content-Length: 55
commitCount: 1409377
nodeName: bedrock1

{"isLeader":"true","state":"LEADING","version":"2.1.0"}
```

### A.2 Request Carrying Fields

A write command. The body is empty, so every parameter travels as a field:

```
CreateThing
Content-Length: 0
name: widget
timeout: 30000
writeConsistency: 2

```

```
200 OK
Content-Length: 19
commitCount: 1409378
nodeName: bedrock1

{"thingID":"90210"}
```

### A.3 Query, Two Output Formats

The method-line shorthand of section 8.1, with the default human-readable
rendering:

```
Query: SELECT 1 AS foo, 2 AS bar;
Content-Length: 0

```

```
200 OK
Content-Length: 16
commitCount: 1409377
nodeName: bedrock1

foo | bar
1 | 2
```

The same query with `Format: json`, using the field form of the query:

```
Query
Content-Length: 0
Format: json
query: SELECT 1 AS foo, 2 AS bar;

```

```
200 OK
Content-Length: 40
commitCount: 1409377
nodeName: bedrock1

{"headers":["foo","bar"],"rows":[[1,2]]}
```

### A.4 Escaped Field Value and Bound Parameters

A JSON document carried in a field value. The logical value is:

```json
{"path":"C:\\tmp","note":"line one\nline two"}
```

On the wire it is one physical line. Every literal backslash is doubled, so the
JSON's own `\\` becomes `\\\\` and its `\n` becomes `\\n`:

```
CreateThing
Content-Length: 0
data: {"path":"C:\\\\tmp","note":"line one\\nline two"}

```

A bound parameter in the same style. The parameter `:name` becomes the field
name `sql-param-#3Aname`, because `:` is 0x3A and section 6.2 encodes it as
`#3A`. The value is a type tag followed by a base64 payload, `T` for text:

```
Query
Content-Length: 0
query: SELECT * FROM thing WHERE name = :name;
sql-param-#3Aname: TZm9vYmFy

```

### A.5 Fire and Forget

```
SomeSlowCommand
Connection: forget
Content-Length: 0

```

The response is sent before the command runs, and the node closes the connection
immediately after. The real outcome is never delivered:

```
202 Successfully queued
Connection: close
Content-Length: 0

```

### A.6 Error Response

A `402` from the `Query` command, with the `error` field and a caret diagram in
the body:

```
402 Bad query
Content-Length: 52
commitCount: 1409377
error: near "SELCT": syntax error
nodeName: bedrock1

near "SELCT": syntax error
SELCT 1;
^--- error here
```

### A.7 Nested COMMIT Messages

A `SYNCHRONIZE_RESPONSE` carrying two commits (section 13.5). The body is two
complete messages, each self-delimited by its own `Content-Length`:

```
SYNCHRONIZE_RESPONSE
CommitCount: 1409377
Content-Length: 218
Hash: 5b8f1c0a9e2d4f7b
NumCommits: 2
commandAddress: 10.0.1.11:8889

COMMIT
CommitIndex: 1409376
Content-Length: 33
Hash: 1a2b3c4d5e6f7a8b

INSERT INTO thing VALUES(1, 'a');COMMIT
CommitIndex: 1409377
Content-Length: 33
Hash: 5b8f1c0a9e2d4f7b

INSERT INTO thing VALUES(2, 'b');
```

Note the line reading `INSERT INTO thing VALUES(1, 'a');COMMIT`. That is not one
line: the first commit's 33-octet body ends after the semicolon, and the second
`COMMIT` message begins immediately, with no separator of any kind. A receiver
finds the boundary only by counting `Content-Length` octets.

In production a `COMMIT` body is normally zstd-compressed and so not printable.
Section 13.7 requires receivers to accept both forms.

---

## Appendix B. Conformance Requirements

Behaviour an independent implementation must reproduce. Each entry gives an
input and the required outcome, with the section that states the rule.

Line endings are written explicitly here, because several entries depend on
them.

### B.1 Mixed Line Endings, section 5.2

Input, in order: `some method line` CRLF, `header1: value1` CR,
`header2: value2` LF, `header3: value3` CRLF, CRLF, `this is the body`.

Required: the whole input is consumed as one message. Method line
`some method line`; `header1` is `value1`, `header2` is `value2`, `header3` is
`value3`; body is `this is the body`.

### B.2 Insufficient Body, section 4.3

Input: `some method line` CRLF, `Content-Length: 100` CRLF, CRLF, `too short`.

Required: incomplete. Nothing consumed, no outputs produced.

### B.3 Content-Length Shorter Than Available, section 4.3

Input: `some method line` CRLF, `Content-Length: 5` CRLF, CRLF, `too short`.

Required: complete. Body is `too s`. The remaining `hort` stays buffered as the
start of the next message.

### B.4 Chunked, Well Formed, section 7.1

Input: `some method line` CRLF, `Transfer-Encoding: chunked` CRLF, CRLF, then
the chunk sequence `5` / `abcde` / `a; extension text` / `0123456789` / `0`,
then CRLF.

Required: body is `abcde0123456789`. `Transfer-Encoding` is absent from the
resulting field set, and `Content-Length` is `15`.

### B.5 Chunk Size Larger Than Data, section 7.1

Input declares a chunk of 6 octets but supplies 5.

Required: incomplete. Nothing consumed.

### B.6 Last Chunk Without Trailing Blank Line, section 7.1

Input ends with the `0` chunk-size line and no blank line after it.

Required: incomplete. Nothing consumed.

### B.7 Trailers Overwrite Fields, section 7.1

Input sets `header2: value2` before the chunked body, and `header2: value2a` as
a trailer after the last chunk.

Required: `header2` is `value2a`.

### B.8 Chunk Size Too Long, section 7.1

Input declares a chunk of `aa` hex, 170 octets, but supplies 10.

Required: incomplete. Nothing consumed.

### B.9 Invalid Hex Chunk Size, sections 7.1 and 15.4

Input has a chunk-size line of `az; extension text` between a valid 5-octet
chunk and 10 octets of data.

Current behaviour: the parse succeeds with a body of just `abcde`, because the
`az` line was reinterpreted as a trailer. This entry documents defect 15.4. A
corrected implementation MAY reject the message instead.

### B.10 Complete Field Section, Both Terminators, section 4.2

For each of CRLF and LF as the line ending: `GET / HTTP/1.1`, then
`Content-Length: 0`, then a blank line.

Required: recognised as having a complete field section.

### B.11 Incomplete Field Section, Both Terminators, section 4.2

For each of CRLF and LF as the line ending: `GET / HTTP/1.1`, then
`Content-Length: 0`, with no blank line.

Required: not recognised as having a complete field section.

### B.12 Method Line Control Character Rejection, sections 8.2 and 14.1

Compose a message whose method line is `500 Internal Server Error`, a CRLF, and
then `Content-Type: application/json`.

Required: composition fails. The implementation MUST NOT emit the injected
field.

### B.13 Content-Length Always Emitted, section 4.4

Compose a message with an empty body and no `Content-Length` set.

Required: the output contains `Content-Length: 0`.

### B.14 Caller Content-Length Discarded, section 4.4

Compose a message with a 10-octet body and `Content-Length: 999` set.

Required: the output contains `Content-Length: 10`.

---

## Appendix C. Implementation Map

Where each part of the protocol lives in the Bedrock source. This is the only
part of the document that names internal functions, and nothing here is
normative. Functions and files rather than line numbers, so that the table
survives ordinary code movement.

### C.1 Core Format

| Concern | File | Function or symbol |
|---|---|---|
| Message structure | `libstuff/SData.h` | `SData` |
| Parse | `libstuff/libstuff.cpp` | `SParseHTTP` |
| Compose | `libstuff/libstuff.cpp` | `SComposeHTTP` |
| Field name and value split | `libstuff/libstuff.cpp` | `_SParseHTTP_GetUpToNext`, `_SParseHTTP_GetUpToEnd` |
| Value escaping | `libstuff/libstuff.cpp` | `SEscape`, `SUnescape` |
| Case-insensitive field names | `libstuff/libstuff.cpp` | `STableComp` |
| Field table type | `libstuff/libstuff.h` | `STable` |
| Blank-line pre-scan | `libstuff/SFastBuffer.cpp` | `SFastBuffer::startsWithHTTPRequest` |
| Buffer management | `libstuff/SFastBuffer.cpp` | `consumeFront`, `append` |
| Status line from a failure | `libstuff/libstuff.h` | `SException`, `STHROW` |
| Request method line parse | `libstuff/libstuff.cpp` | `SParseRequestMethodLine` |
| Response status parse (see 15.7) | `libstuff/libstuff.cpp` | `SParseResponseMethodLine` |
| Bound parameter encoding | `libstuff/SQliteParameter.h` | `serialize`, `uriEncodeParamName` |

### C.2 Client Profile

| Concern | File | Function or symbol |
|---|---|---|
| Connection lifecycle | `BedrockServer.cpp` | `handleSocket` |
| Request to command | `BedrockServer.cpp` | `buildCommandFromRequest` |
| Response send | `BedrockServer.cpp` | `_reply` |
| Status commands | `BedrockServer.cpp` | `_status` |
| Control commands | `BedrockServer.cpp` | `_control` |
| Status and control dispatch | `BedrockServer.cpp` | `_handleIfStatusOrControlCommand` |
| Command port blocking | `BedrockServer.cpp` | `blockCommandPort`, `unblockCommandPort` |
| Crash suppression | `BedrockServer.cpp` | `_wouldCrash` |
| Command phases, default `200 OK`, JSON body | `BedrockCore.cpp` | `peekCommand`, `processCommand` |
| Failure to response | `BedrockCore.cpp` | `_handleCommandException` |
| Timeout derivation | `BedrockCommand.cpp` | `_getTimeout` |
| `requestID` generation, `writeConsistency` parse | `sqlitecluster/SQLiteCommand.cpp` | `preprocessRequest`, constructor |
| Socket send and receive | `libstuff/STCPManager.cpp` | `Socket::send`, `Socket::recv` |
| `Query` command | `plugins/DB.cpp` | `BedrockDBCommand` |
| Plugin port hooks | `BedrockPlugin.h` | `getPort`, `onPortRecv`, `onPortRequestComplete` |
| Size limits | `BedrockPlugin.h` | `MAX_SIZE_QUERY`, `MAX_SIZE_BLOB` |

### C.3 Escalation Profile

| Concern | File | Function or symbol |
|---|---|---|
| Send, receive, and response parse | `sqlitecluster/SQLiteClusterMessenger.cpp` | `_sendCommandOnSocket` |
| Peer selection and retry | `sqlitecluster/SQLiteClusterMessenger.cpp` | `runOnPeer` |
| HTTPS request serialization | `BedrockCommand.cpp` | `serializeHTTPSRequests`, `deserializeHTTPSRequests` |
| Command ID generation | `BedrockServer.cpp` | `generateCommandID` |

### C.4 Replication Profile

| Concern | File | Function or symbol |
|---|---|---|
| Message dispatch | `sqlitecluster/SQLiteNode.cpp` | `_onMESSAGE` |
| Universal field injection | `sqlitecluster/SQLiteNode.cpp` | `_addPeerHeaders` |
| Send to one peer, send to all | `sqlitecluster/SQLiteNode.cpp` | `_sendToPeer`, `_sendToAllPeers` |
| `LOGIN` construction | `sqlitecluster/SQLiteNode.cpp` | `_onConnect` |
| Connection acceptance | `sqlitecluster/SQLiteNode.cpp` | `postPoll` |
| Per-peer framing | `sqlitecluster/SQLitePeer.cpp` | `popMessage` |
| `COMMIT` nesting and validation | `sqlitecluster/SQLiteNode.cpp` | `_queueSynchronize`, `_recvSynchronize` |
| Transaction phases | `sqlitecluster/SQLiteNode.cpp` | `_handleBeginTransaction`, `_handlePrepareTransaction`, `_handleCommitTransaction`, `_handleRollbackTransaction` |
| Async transaction send | `sqlitecluster/SQLiteNode.cpp` | `_sendOutstandingTransactions` |
| `STANDUP_RESPONSE`, `PING` | `sqlitecluster/SQLiteNode.cpp` | `_sendStandupResponse`, `_sendPING` |
| `STATE` broadcast | `sqlitecluster/SQLiteNode.cpp` | `_changeState` |
| State names | `sqlitecluster/SQLiteNode.cpp` | `stateName`, `stateFromName` |
| Timing constants | `sqlitecluster/SQLiteNode.cpp` | `RECV_TIMEOUT`, `MIN_APPROVE_FREQUENCY` |
| Peer state | `sqlitecluster/SQLitePeer.h` | `SQLitePeer` |

### C.5 Tests

| Concern | File |
|---|---|
| Parser conformance, B.1 to B.9 | `test/tests/LibStuffTest.cpp` |
| Method line injection, B.12 | `test/tests/LibStuffTest.cpp` |
| Blank-line pre-scan, B.10 and B.11 | `test/tests/FastHTTPParsing.cpp` |

### C.6 Related Documents

| Topic | Location |
|---|---|
| Overview and `netcat` walkthrough | `docs/index.md` |
| Consensus algorithm | `docs/synchronization.md` |
| Command line arguments | `docs/cli.md` |
| Jobs plugin commands | `docs/jobs.md` |
| Cache plugin commands | `docs/cache.md` |

<!-- vim: set filetype=markdown textwidth=80: -->
