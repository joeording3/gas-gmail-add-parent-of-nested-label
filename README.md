# gas-gmail-add-parent-of-nested-label

Google Apps Script that adds ancestor labels to Gmail messages labeled with one of their nested descendants.

This version:

- adds **all ancestor labels**, not just the immediate parent
- supports **skip lists**
- supports **optional system-label filtering**
- processes messages **incrementally page by page**
- uses **batched label updates**
- uses **retry with exponential backoff**
- supports **automatic continuation across runs** for large backlogs
- stops cleanly when a **message cap** or **time budget** is reached

The goal of this script is to create a Gmail label search experience that behaves more like a directory search, without having to duplicate and combine filter rules manually.

Gmail custom user labels can be nested below one another, and Gmail displays labels in a way that resembles a directory tree. But searching a parent label in Gmail does **not** automatically include its descendants.

This script makes descendant-labeled messages also receive their ancestor labels, so searching a parent label behaves more like an inclusive tree search.

## Example

**Ancestor**  
A label in the same branch that is at least one level higher up, such as a parent or grandparent.

**Descendant**  
A label in the same branch that is at least one level lower down, such as a child or grandchild.

In this menu, the label `eriador` has 4 descendants:

```text
------------
Sent
All Mail
Spam
Drafts
eriador
   > shire
        > westfarthing
   > rivendell
   > breeland
------------
```

**4 descendants of `eriador`**

```text
3 children:   "shire", "rivendell", and "breeland"
1 grandchild: "westfarthing"
```

**Also note**

```text
"shire" is an ancestor of "westfarthing"
"westfarthing" is a descendant of both "shire" and "eriador"
```

### Search Eriador for Isildur's Bane

Without this script:

```text
"Isildur's Bane" label:(eriador OR eriador/shire OR eriador/shire/westfarthing OR eriador/rivendell OR eriador/breeland)
```

With this script:

```text
"Isildur's Bane" label:eriador
```

## What this version does

If a message has:

```text
sport/hockey
```

the script adds:

```text
sport
```

If a message has:

```text
sport/lax/field
```

the script adds:

```text
sport
sport/lax
```

It only adds missing ancestor labels. It does not remove any labels.

## Features

### Full ancestor propagation

For nested labels, this version adds **all missing ancestors** in the chain.

Examples:

```text
sport/hockey      -> sport
sport/lax/field   -> sport, sport/lax
```

### Skip lists

You can exclude:

- exact labels from processing
- all descendants of selected labels

### Optional system-label filtering

You can restrict processing to messages matching Gmail system-label criteria.

Example:

```javascript
const REQUIRED_SYSTEM_LABELS_ALL = ["UNREAD", "INBOX"];
const EXCLUDED_SYSTEM_LABELS = ["TRASH", "SPAM"];
```

### Incremental processing

This version:

- fetches Gmail search results page by page
- processes each page immediately
- stops cleanly when the run is near its limits
- resumes later from the saved position

### Batched updates

Label updates use Gmail API `batchModify`, split into chunks to stay within Gmail's API limits.

### Retry with exponential backoff

Transient failures in both:

- `messages.list`
- `messages.batchModify`

are retried automatically.

### Trigger-based continuation

If a run cannot finish within one Apps Script execution, the script saves its place and schedules a continuation trigger automatically.

## How continuation works

This version keeps track of:

- current label
- current ancestor
- current Gmail `pageToken`

If a run stops because it reaches either:

- the configured message cap, or
- the configured time budget

it saves state and schedules another run.

The next run resumes from the same point, including mid-query when needed.

## Installation

A. Create a Google Apps Script project  
B. Paste the script into `Code.gs`  
C. Add the Advanced Gmail API service  
D. Authorize the project  
E. Create an initial trigger

### A. Create Google Apps Script project

1. Sign in to your Google account.
2. Go to `script.google.com`.
3. Click **New project**.
4. Give the project a name.

### B. Paste the code

5. Delete any pre-populated code in the editor.
6. Paste the script into `Code.gs`.
7. Save the project.

### C. Add dependency

8. In the Apps Script editor, click **Services**.
9. Add the **Gmail API** advanced service.

### D. Authorize project

10. In the editor, select the `addParentLabel` function.
11. Click **Run**.
12. Review and grant permissions.

### E. Create trigger

#### Option 1: Manual start only

Run `addParentLabel()` manually whenever you want to start a full pass.  
If the mailbox is large, the script will schedule continuation triggers automatically as needed.

#### Option 2: Recurring scheduled runs

Create a time-driven trigger for `addParentLabel`.

Suggested settings:

```text
Choose which function to run:       addParentLabel
Choose which deployment should run: Head
Select event source:                Time-driven
```

## Configuration

Edit the constants near the top of the script.

### Skip lists

#### Exclude descendants of these labels

```javascript
const OFFSPRING_SKIP_LIST = [
  // "auctions",
];
```

#### Exclude these exact labels

```javascript
const LABEL_SKIP_LIST = [
  // "shopping/amazon",
];
```

### System-label filters

#### Any of these must match

```javascript
const REQUIRED_SYSTEM_LABELS_ANY = [
  // "UNREAD",
];
```

#### All of these must match

```javascript
const REQUIRED_SYSTEM_LABELS_ALL = [
  // "INBOX",
];
```

#### None of these may match

```javascript
const EXCLUDED_SYSTEM_LABELS = [
  "TRASH",
  "SPAM",
];
```

Examples:

Only unread inbox messages:

```javascript
const REQUIRED_SYSTEM_LABELS_ANY = [];
const REQUIRED_SYSTEM_LABELS_ALL = ["UNREAD", "INBOX"];
const EXCLUDED_SYSTEM_LABELS = ["TRASH", "SPAM"];
```

Only unread or starred messages:

```javascript
const REQUIRED_SYSTEM_LABELS_ANY = ["UNREAD", "STARRED"];
const REQUIRED_SYSTEM_LABELS_ALL = [];
const EXCLUDED_SYSTEM_LABELS = ["TRASH", "SPAM"];
```

Process everything except trash and spam:

```javascript
const REQUIRED_SYSTEM_LABELS_ANY = [];
const REQUIRED_SYSTEM_LABELS_ALL = [];
const EXCLUDED_SYSTEM_LABELS = ["TRASH", "SPAM"];
```

### Logging

```text
1 = info
2 = verbose
3 = debug
```

```javascript
const LOG_LEVEL = 1;
```

### Dry run

```javascript
const DRY_RUN = true;
```

### Processing limits

```javascript
const MAX_MESSAGES_PER_RUN = 2000;
const MAX_RUNTIME_MS = 4.5 * 60 * 1000;
```

### Gmail API page and batch sizes

```javascript
const SEARCH_PAGE_SIZE = 500;
const BATCH_MODIFY_SIZE = 1000;
```

### Retry settings

```javascript
const RETRY_MAX_ATTEMPTS = 3;
const RETRY_INITIAL_DELAY_MS = 500;
```

### Continuation delay

```javascript
const CONTINUATION_DELAY_MS = 60 * 1000;
```

## Operational notes

### Large mailboxes

If you have many nested labels and many matching messages, the script may take multiple runs to complete one full pass.

### Repeated runs are safe

The script only targets messages that match:

- the child label
- not the ancestor label yet

Once an ancestor label has been added, that message no longer matches that query.

### Page-token continuation

This version resumes mid-query using Gmail `pageToken`. If Gmail's result set changes between runs, the exact paging sequence may shift, but the script remains safe because already-updated messages drop out of the `-label:"ancestor"` query.

## Recommended usage

1. Set `DRY_RUN = false`
2. Leave `MAX_MESSAGES_PER_RUN` modest, such as `1000` or `2000`
3. Run `addParentLabel()`
4. Let continuation triggers finish the backlog

After the backlog is cleared, a periodic trigger can keep labels synchronized incrementally.

## Troubleshooting

### "Exceeded maximum execution time"

Lower:

- `MAX_MESSAGES_PER_RUN`

and keep continuation enabled.

### "Empty response" or transient Gmail API failures

The script retries `messages.list` and `batchModify` automatically with exponential backoff.

### Missing ancestor label

If a nested label exists but one of its ancestor labels does not actually exist as a Gmail label, that ancestor is skipped and logged.

### Too many messages for one run

The script will stop early, save state, and schedule continuation.

## Feedback

Constructive feedback and bug reports are welcome.
