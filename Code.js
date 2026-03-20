/**
 * Gmail Add Parent Labels for Nested Labels
 * ----------------------------------------
 *
 * Purpose
 * -------
 * For Gmail messages that have nested user labels, ensure that all ancestor
 * labels are also applied.
 *
 * Examples
 * --------
 *   "sport/hockey"      -> also add "sport"
 *   "sport/basketball"  -> also add "sport"
 *   "sport/lax/field"   -> also add "sport" and "sport/lax"
 *
 * Features
 * --------
 * - Skips labels listed in LABEL_SKIP_LIST
 * - Skips descendants of labels listed in OFFSPRING_SKIP_LIST
 * - Optional filtering based on Gmail system labels/search operators
 * - Processes messages incrementally page by page
 * - Resumes mid-query using Gmail pageToken
 * - Uses batchModify for performance
 * - Chunks batchModify to Gmail API's 1000-ID limit
 * - Retries failed messages.list and batchModify calls with exponential backoff
 * - Stops when message cap or time budget is reached
 * - Automatically schedules continuation runs for large backlogs
 *
 * Requirements
 * ------------
 * 1. Enable the Advanced Gmail service in Apps Script:
 *      Services -> Gmail API -> On
 * 2. Also ensure the Gmail API is enabled in the linked Google Cloud project.
 *
 * Notes
 * -----
 * - Only user labels are processed as source labels.
 * - System labels are ignored as source labels.
 * - The script adds missing ancestor labels but never removes labels.
 * - Continuation can resume at the exact label/ancestor/pageToken position.
 */

/* ========================================================================== */
/* Configuration                                                               */
/* ========================================================================== */

/**
 * Labels whose descendants should be excluded from syncing.
 *
 * Example:
 *   If "auctions" is in this list, then:
 *     "auctions/ebay"
 *   is excluded.
 *
 * Note:
 * - The label itself is NOT excluded unless it is also listed in LABEL_SKIP_LIST.
 */
const OFFSPRING_SKIP_LIST = [
//  "auctions",
];

/**
 * Individual labels to exclude directly.
 *
 * Example:
 *   If "shopping/amazon" is in this list, that exact label is skipped.
 */
const LABEL_SKIP_LIST = [
//  "shopping/amazon",
];

/**
 * Optional Gmail search filters using system labels / operators.
 *
 * These are appended to every Gmail search query used to find messages
 * that need ancestor labels added.
 *
 * Examples:
 *   REQUIRED_SYSTEM_LABELS_ALL = ["UNREAD", "INBOX"]
 *   REQUIRED_SYSTEM_LABELS_ANY = ["UNREAD", "STARRED"]
 *   EXCLUDED_SYSTEM_LABELS = ["TRASH", "SPAM"]
 */
const REQUIRED_SYSTEM_LABELS_ANY = [
  // "UNREAD",
];

const REQUIRED_SYSTEM_LABELS_ALL = [
  // "INBOX",
];

const EXCLUDED_SYSTEM_LABELS = [
  "TRASH",
  "SPAM",
];

/**
 * Logging verbosity.
 * 1 = info
 * 2 = verbose
 * 3 = debug
 */
const LOG_LEVEL = 1;

/**
 * When true, the script logs intended work but does not modify Gmail.
 */
const DRY_RUN = false;

/**
 * Maximum number of messages returned per Gmail.Users.Messages.list call.
 *
 * Gmail currently allows up to 500 here.
 */
const SEARCH_PAGE_SIZE = 500;

/**
 * Maximum number of message IDs allowed per Gmail.Users.Messages.batchModify call.
 *
 * Gmail currently limits this to 1000.
 */
const BATCH_MODIFY_SIZE = 1000;

/**
 * Safety cap for one execution.
 *
 * Once this many messages have been updated in a run, the script stops and
 * schedules a continuation.
 */
const MAX_MESSAGES_PER_RUN = 2000;

/**
 * Soft execution budget in milliseconds.
 *
 * Stop before Apps Script's hard runtime limit so state can be saved and
 * a continuation trigger can be scheduled cleanly.
 */
const MAX_RUNTIME_MS = 4.5 * 60 * 1000; // 4.5 minutes

/**
 * Delay before continuation trigger fires, in milliseconds.
 */
const CONTINUATION_DELAY_MS = 60 * 1000;

/**
 * Retry configuration for transient API failures.
 */
const RETRY_MAX_ATTEMPTS = 3;
const RETRY_INITIAL_DELAY_MS = 500;

/**
 * Prefix used for state stored in Script Properties.
 */
const STATE_KEY = "GMAIL_PARENT_LABEL_SYNC_STATE";

/**
 * Name of the triggerable entrypoint used for continuation.
 */
const CONTINUATION_FUNCTION_NAME = "resumeAddParentLabel";

/* ========================================================================== */
/* Entry points                                                                */
/* ========================================================================== */

/**
 * Main entrypoint.
 *
 * Starts a fresh run from the beginning of the eligible label list.
 * Clears any previous continuation state before starting.
 */
function addParentLabel() {
  clearContinuationState();
  runAddParentLabel({ resume: false });
}

/**
 * Continuation entrypoint.
 *
 * Called by a time-based trigger when a prior run hit the message cap or time
 * budget and scheduled a continuation.
 */
function resumeAddParentLabel() {
  runAddParentLabel({ resume: true });
}

/* ========================================================================== */
/* Main workflow                                                               */
/* ========================================================================== */

/**
 * Core runner for both fresh and resumed executions.
 *
 * @param {Object} options
 * @param {boolean} options.resume Whether this run is resuming from saved state
 */
function runAddParentLabel(options) {
  const startedAt = Date.now();
  const logLevel = normalizeLogLevel(LOG_LEVEL);
  const resume = Boolean(options && options.resume);

  const gmailLabels = Gmail.Users.Labels.list("me").labels || [];
  const userLabels = gmailLabels.filter(label => label.type === "user");
  const systemLabels = gmailLabels.filter(label => label.type === "system");

  const allLabelNames = gmailLabels.map(label => label.name);
  const allLabelNameSet = new Set(allLabelNames);
  const systemLabelNameSet = new Set(systemLabels.map(label => label.name));
  const labelMap = new Map(gmailLabels.map(label => [label.name, label]));
  const skippedLabels = new Set(LABEL_SKIP_LIST);

  logInfo(`Retrieved ${gmailLabels.length} Gmail labels.`);
  logInfo(`${systemLabels.length} system labels ignored as source labels.`);
  logInfo(`${userLabels.filter(label => !label.name.includes("/")).length} user labels have no parent.`);

  validateSkipList("Offspring", OFFSPRING_SKIP_LIST, allLabelNameSet, systemLabelNameSet);
  validateSkipList("Labels", LABEL_SKIP_LIST, allLabelNameSet, systemLabelNameSet);
  validateSystemLabelFilters(systemLabelNameSet);

  const systemFilterQuery = buildSystemLabelFilterQuery();
  if (systemFilterQuery) {
    logInfo(`System-label filter active: ${systemFilterQuery}`);
  }

  const nestedUserLabels = userLabels.filter(label => label.name.includes("/"));
  const skipMatches = [];
  const eligibleLabels = [];

  for (const label of nestedUserLabels) {
    const reason = getSkipReason(label.name, skippedLabels, OFFSPRING_SKIP_LIST);
    if (reason) {
      skipMatches.push({ label: label.name, reason });
    } else {
      eligibleLabels.push(label);
    }
  }

  eligibleLabels.sort((a, b) => a.name.localeCompare(b.name));

  if (skipMatches.length > 0) {
    logInfo(`${skipMatches.length} nested user labels match skip lists.`);
    if (logLevel >= 2) {
      logVerbose(formatSkipMatches(skipMatches));
    }
  }

  logInfo(`${eligibleLabels.length} user labels to search.`);

  if (logLevel >= 2) {
    const names = eligibleLabels.map(label => label.name);
    logVerbose("Checking labels:\n" + names.map(name => `  - ${name}`).join("\n"));
  }

  const savedState = resume ? loadContinuationState() : null;
  const startPosition = getStartPosition(savedState, eligibleLabels);

  if (resume) {
    if (savedState) {
      logInfo(
        `Resuming from label index ${startPosition.labelIndex + 1}, ancestor index ${startPosition.ancestorIndex + 1}` +
        `${startPosition.pageToken ? " with saved pageToken." : "."}`
      );
    } else {
      logInfo("No saved continuation state found. Starting from the beginning.");
    }
  }

  let labelsChecked = 0;
  let updateGroups = 0;
  let totalMessagesTouched = 0;
  let totalBatchCalls = 0;
  let totalBatchFailures = 0;
  let totalListFailures = 0;
  let stoppedEarly = false;
  let nextState = null;

  for (let labelIndex = startPosition.labelIndex; labelIndex < eligibleLabels.length; labelIndex++) {
    const label = eligibleLabels[labelIndex];
    labelsChecked += 1;

    const ancestorNames = getAncestorNames(label.name);
    if (ancestorNames.length === 0) {
      continue;
    }

    const ancestorStartIndex =
      labelIndex === startPosition.labelIndex ? startPosition.ancestorIndex : 0;

    if (logLevel >= 3) {
      logDebug(`Checking "${label.name}" with ancestors: ${ancestorNames.join(", ")}`);
    }

    for (let ancestorIndex = ancestorStartIndex; ancestorIndex < ancestorNames.length; ancestorIndex++) {
      const ancestorName = ancestorNames[ancestorIndex];
      const ancestorLabel = labelMap.get(ancestorName);

      if (!ancestorLabel) {
        logInfo(`Skipping "${label.name}" -> missing ancestor label "${ancestorName}".`);
        continue;
      }

      if (shouldStopNow(startedAt, totalMessagesTouched)) {
        stoppedEarly = true;
        nextState = buildContinuationState(
          label.name,
          ancestorName,
          labelIndex,
          ancestorIndex,
          null
        );
        logInfo(
          `Stopping before processing "${label.name}" -> "${ancestorName}" due to time/message budget. ` +
          `No messages for this pair were changed in this run. Scheduling continuation.`
        );
        break;
      }

      const query = buildMessageSearchQuery(label.name, ancestorName, systemFilterQuery);

      const startingPageToken =
        labelIndex === startPosition.labelIndex && ancestorIndex === startPosition.ancestorIndex
          ? startPosition.pageToken
          : null;

      const remainingBudget = MAX_MESSAGES_PER_RUN - totalMessagesTouched;

      const result = DRY_RUN
        ? processQueryPagedDryRun(query, startedAt, remainingBudget, startingPageToken)
        : processQueryPaged(query, ancestorLabel.id, startedAt, remainingBudget, startingPageToken);

      totalListFailures += result.listFailureCount || 0;
      totalBatchCalls += result.batchCount || 0;
      totalBatchFailures += result.failureCount || 0;
      totalMessagesTouched += result.touchedCount || 0;

      if (result.touchedCount > 0) {
        updateGroups += 1;

        if (DRY_RUN) {
          logInfo(
            `[DRY RUN] Would add "${ancestorName}" to ${result.touchedCount} message(s) labeled "${label.name}".`
          );
        } else {
          logInfo(
            `Added "${ancestorName}" to ${result.touchedCount} message(s) labeled "${label.name}" ` +
            `in ${result.batchCount} batch(es)` +
            `${result.failureCount > 0 ? ` with ${result.failureCount} failed batch(es)` : ""}.`
          );
        }

        if (logLevel >= 2 && result.previewIds && result.previewIds.length > 0) {
          logVerbose(buildMessagePreview(label.name, ancestorName, result.previewIds));
        }
      } else if (result.completed && logLevel >= 3) {
        logDebug(`No messages missing "${ancestorName}" for "${label.name}".`);
      }

      if (!result.completed) {
        stoppedEarly = true;
        nextState = buildContinuationState(
          label.name,
          ancestorName,
          labelIndex,
          ancestorIndex,
          result.nextPageToken || null
        );

        logInfo(
          `Stopped partway through "${label.name}" -> "${ancestorName}" due to time/message budget. ` +
          `Scheduling continuation from the saved page position.`
        );
        break;
      }
    }

    if (stoppedEarly) {
      break;
    }
  }

  if (stoppedEarly && nextState) {
    saveContinuationState(nextState);
    scheduleContinuationTrigger();
  } else {
    clearContinuationState();
    deleteContinuationTriggers();
  }

  if (totalMessagesTouched === 0 && !stoppedEarly) {
    logInfo("All labeled messages already included the labels of their ancestors.");
    return;
  }

  logInfo(
    `${DRY_RUN ? "[DRY RUN] " : ""}Done. Checked ${labelsChecked} label(s), ` +
    `performed ${updateGroups} update group(s), touched ${totalMessagesTouched} message(s)` +
    `${DRY_RUN
      ? `, with ${totalListFailures} messages.list failure(s).`
      : `, across ${totalBatchCalls} batchModify call(s), with ${totalBatchFailures} failed batch(es) and ${totalListFailures} messages.list failure(s).`
    }`
  );
}

/* ========================================================================== */
/* Query building                                                              */
/* ========================================================================== */

/**
 * Build the Gmail query used to find messages that:
 * - have the nested child label
 * - do not yet have the ancestor label
 * - optionally match configured system-label filters
 *
 * @param {string} childLabelName Nested child label
 * @param {string} ancestorLabelName Missing ancestor label
 * @param {string} systemFilterQuery Prebuilt optional system-filter clause
 * @returns {string} Gmail search query
 */
function buildMessageSearchQuery(childLabelName, ancestorLabelName, systemFilterQuery) {
  const parts = [
    `label:"${childLabelName}"`,
    `-label:"${ancestorLabelName}"`,
  ];

  if (systemFilterQuery) {
    parts.push(systemFilterQuery);
  }

  return parts.join(" ");
}

/**
 * Build the optional Gmail search clause for configured system-label filters.
 *
 * @returns {string} Gmail search fragment, or empty string if no filters
 */
function buildSystemLabelFilterQuery() {
  const parts = [];

  if (REQUIRED_SYSTEM_LABELS_ALL.length > 0) {
    for (const label of REQUIRED_SYSTEM_LABELS_ALL) {
      parts.push(`label:"${label}"`);
    }
  }

  if (REQUIRED_SYSTEM_LABELS_ANY.length > 0) {
    const anyGroup = REQUIRED_SYSTEM_LABELS_ANY
      .map(label => `label:"${label}"`)
      .join(" OR ");

    parts.push(`{${anyGroup}}`);
  }

  if (EXCLUDED_SYSTEM_LABELS.length > 0) {
    for (const label of EXCLUDED_SYSTEM_LABELS) {
      parts.push(`-label:"${label}"`);
    }
  }

  return parts.join(" ").trim();
}

/**
 * Validate configured system-label filters against the system labels returned
 * by the Gmail Labels API.
 *
 * @param {Set<string>} systemLabelNameSet Known system labels from Gmail
 */
function validateSystemLabelFilters(systemLabelNameSet) {
  const allConfigured = [
    ...REQUIRED_SYSTEM_LABELS_ANY,
    ...REQUIRED_SYSTEM_LABELS_ALL,
    ...EXCLUDED_SYSTEM_LABELS,
  ];

  if (allConfigured.length === 0) {
    return;
  }

  const uniqueConfigured = [...new Set(allConfigured)];
  const missing = uniqueConfigured.filter(name => !systemLabelNameSet.has(name));

  if (missing.length > 0) {
    logInfo(
      `Warning: ${missing.length} configured system label filter(s) were not found in Gmail system labels: ` +
      `[ ${missing.join(", ")} ]`
    );
  }
}

/* ========================================================================== */
/* Label and skip logic                                                        */
/* ========================================================================== */

/**
 * Normalize LOG_LEVEL to a safe value.
 *
 * @param {number} level Proposed log level
 * @returns {number} 1, 2, or 3
 */
function normalizeLogLevel(level) {
  return [1, 2, 3].includes(level) ? level : 1;
}

/**
 * Return all ancestor label names for a nested label.
 *
 * Example:
 *   "sport/lax/field" -> ["sport", "sport/lax"]
 *
 * @param {string} labelName Nested label name
 * @returns {string[]} Ancestor label names from highest to nearest parent
 */
function getAncestorNames(labelName) {
  const parts = labelName.split("/");
  const ancestors = [];

  for (let i = 1; i < parts.length; i++) {
    ancestors.push(parts.slice(0, i).join("/"));
  }

  return ancestors;
}

/**
 * Determine whether a label should be skipped and why.
 *
 * @param {string} labelName Label to test
 * @param {Set<string>} skippedLabels Exact labels to skip
 * @param {string[]} skippedAncestors Ancestor labels whose descendants are skipped
 * @returns {string|null} Human-readable reason, or null if not skipped
 */
function getSkipReason(labelName, skippedLabels, skippedAncestors) {
  if (skippedLabels.has(labelName)) {
    return "Label skip list";
  }

  for (const ancestor of skippedAncestors) {
    if (labelName.startsWith(ancestor + "/")) {
      return `Offspring of "${ancestor}"`;
    }
  }

  return null;
}

/* ========================================================================== */
/* Incremental Gmail processing                                                */
/* ========================================================================== */

/**
 * Return true when the current run should stop soon.
 *
 * @param {number} startedAt Epoch milliseconds when the run began
 * @param {number} totalMessagesTouched Number of messages updated so far in this run
 * @returns {boolean}
 */
function shouldStopNow(startedAt, totalMessagesTouched) {
  return (
    totalMessagesTouched >= MAX_MESSAGES_PER_RUN ||
    Date.now() - startedAt >= MAX_RUNTIME_MS
  );
}

/**
 * Process one label/ancestor query incrementally, page by page.
 *
 * This is the key timeout fix:
 * - it does not collect all IDs first
 * - it processes each page immediately
 * - it stops cleanly if time/message budget is reached
 * - it returns nextPageToken so the next run resumes the same query
 *
 * @param {string} query Gmail search query
 * @param {string} ancestorLabelId Label ID to add
 * @param {number} startedAt Epoch milliseconds when the run began
 * @param {number} remainingMessageBudget Remaining per-run message budget
 * @param {string|null} startingPageToken Saved page token for continuation
 * @returns {{
 *   touchedCount: number,
 *   batchCount: number,
 *   failureCount: number,
 *   listFailureCount: number,
 *   completed: boolean,
 *   nextPageToken: (string|null),
 *   previewIds: string[]
 * }}
 */
function processQueryPaged(query, ancestorLabelId, startedAt, remainingMessageBudget, startingPageToken) {
  let pageToken = startingPageToken || null;
  let touchedCount = 0;
  let batchCount = 0;
  let failureCount = 0;
  let listFailureCount = 0;
  const previewIds = [];

  while (true) {
    if (shouldStopNow(startedAt, touchedCount)) {
      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: false,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    if (touchedCount >= remainingMessageBudget) {
      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: false,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    const listResult = retryListMessagesPage(query, pageToken);

    if (!listResult.ok) {
      listFailureCount += 1;

      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: touchedCount > 0 ? false : true,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    const response = listResult.response;
    const messages = Array.isArray(response.messages) ? response.messages : [];
    const nextPageToken = response.nextPageToken || null;

    if (messages.length === 0) {
      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: true,
        nextPageToken: null,
        previewIds,
      };
    }

    const roomLeft = remainingMessageBudget - touchedCount;
    const limitedMessages = messages.slice(0, roomLeft);
    const ids = limitedMessages.map(message => message.id);

    if (previewIds.length < 20) {
      const needed = 20 - previewIds.length;
      previewIds.push(...ids.slice(0, needed));
    }

    const batchResult = batchModifyMessagesSafe(ids, [ancestorLabelId], []);
    touchedCount += ids.length;
    batchCount += batchResult.batchCount;
    failureCount += batchResult.failureCount;

    if (limitedMessages.length < messages.length) {
      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: false,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    if (shouldStopNow(startedAt, touchedCount)) {
      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: false,
        nextPageToken: nextPageToken,
        previewIds,
      };
    }

    if (!nextPageToken) {
      return {
        touchedCount,
        batchCount,
        failureCount,
        listFailureCount,
        completed: true,
        nextPageToken: null,
        previewIds,
      };
    }

    pageToken = nextPageToken;
  }
}

/**
 * Dry-run version of processQueryPaged.
 *
 * Reads query pages incrementally and counts what would be processed, but does
 * not modify any messages.
 *
 * @param {string} query Gmail search query
 * @param {number} startedAt Epoch milliseconds when the run began
 * @param {number} remainingMessageBudget Remaining per-run message budget
 * @param {string|null} startingPageToken Saved page token for continuation
 * @returns {{
 *   touchedCount: number,
 *   batchCount: number,
 *   failureCount: number,
 *   listFailureCount: number,
 *   completed: boolean,
 *   nextPageToken: (string|null),
 *   previewIds: string[]
 * }}
 */
function processQueryPagedDryRun(query, startedAt, remainingMessageBudget, startingPageToken) {
  let pageToken = startingPageToken || null;
  let touchedCount = 0;
  let listFailureCount = 0;
  const previewIds = [];

  while (true) {
    if (shouldStopNow(startedAt, touchedCount)) {
      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: false,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    if (touchedCount >= remainingMessageBudget) {
      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: false,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    const listResult = retryListMessagesPage(query, pageToken);

    if (!listResult.ok) {
      listFailureCount += 1;

      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: touchedCount > 0 ? false : true,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    const response = listResult.response;
    const messages = Array.isArray(response.messages) ? response.messages : [];
    const nextPageToken = response.nextPageToken || null;

    if (messages.length === 0) {
      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: true,
        nextPageToken: null,
        previewIds,
      };
    }

    const roomLeft = remainingMessageBudget - touchedCount;
    const limitedMessages = messages.slice(0, roomLeft);
    const ids = limitedMessages.map(message => message.id);

    touchedCount += ids.length;

    if (previewIds.length < 20) {
      const needed = 20 - previewIds.length;
      previewIds.push(...ids.slice(0, needed));
    }

    if (limitedMessages.length < messages.length) {
      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: false,
        nextPageToken: pageToken,
        previewIds,
      };
    }

    if (shouldStopNow(startedAt, touchedCount)) {
      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: false,
        nextPageToken: nextPageToken,
        previewIds,
      };
    }

    if (!nextPageToken) {
      return {
        touchedCount,
        batchCount: 0,
        failureCount: 0,
        listFailureCount,
        completed: true,
        nextPageToken: null,
        previewIds,
      };
    }

    pageToken = nextPageToken;
  }
}

/**
 * Retry a single Gmail.Users.Messages.list call using exponential backoff.
 *
 * @param {string} query Gmail search query
 * @param {string|null} pageToken Gmail page token
 * @returns {{ok:boolean, response:(Object|null)}}
 */
function retryListMessagesPage(query, pageToken) {
  let lastError = null;

  for (let attempt = 1; attempt <= RETRY_MAX_ATTEMPTS; attempt++) {
    try {
      const response = Gmail.Users.Messages.list("me", {
        q: query,
        pageToken: pageToken || null,
        maxResults: SEARCH_PAGE_SIZE,
      });

      if (!response) {
        if (attempt === RETRY_MAX_ATTEMPTS) {
          logInfo(
            `messages.list returned an empty response after ${RETRY_MAX_ATTEMPTS} attempt(s)` +
            ` for query: ${query}${pageToken ? ` (pageToken: ${pageToken})` : ""}`
          );
          return { ok: false, response: null };
        }

        const delayMs = RETRY_INITIAL_DELAY_MS * Math.pow(2, attempt - 1);
        logInfo(
          `messages.list returned an empty response on attempt ${attempt}/${RETRY_MAX_ATTEMPTS}` +
          ` for query: ${query}${pageToken ? ` (pageToken: ${pageToken})` : ""}. Retrying in ${delayMs} ms.`
        );
        Utilities.sleep(delayMs);
        continue;
      }

      return { ok: true, response: response };
    } catch (err) {
      lastError = err;
      const message = err && err.message ? err.message : String(err);

      if (attempt === RETRY_MAX_ATTEMPTS) {
        logInfo(
          `messages.list failed permanently after ${RETRY_MAX_ATTEMPTS} attempt(s)` +
          ` for query: ${query}${pageToken ? ` (pageToken: ${pageToken})` : ""}: ${message}`
        );
        return { ok: false, response: null };
      }

      const delayMs = RETRY_INITIAL_DELAY_MS * Math.pow(2, attempt - 1);
      logInfo(
        `messages.list failed on attempt ${attempt}/${RETRY_MAX_ATTEMPTS}` +
        ` for query: ${query}${pageToken ? ` (pageToken: ${pageToken})` : ""}: ${message}.` +
        ` Retrying in ${delayMs} ms.`
      );
      Utilities.sleep(delayMs);
    }
  }

  logDebug(
    `messages.list last error: ${lastError && lastError.message ? lastError.message : lastError}`
  );

  return { ok: false, response: null };
}

/**
 * Add/remove labels in Gmail batchModify calls, chunked to Gmail's 1000-ID limit.
 *
 * @param {string[]} messageIds Message IDs to modify
 * @param {string[]} addLabelIds Label IDs to add
 * @param {string[]} removeLabelIds Label IDs to remove
 * @returns {{batchCount:number, failureCount:number}} Summary of batch attempts
 */
function batchModifyMessagesSafe(messageIds, addLabelIds, removeLabelIds) {
  if (!messageIds || messageIds.length === 0) {
    return { batchCount: 0, failureCount: 0 };
  }

  let batchCount = 0;
  let failureCount = 0;

  for (let i = 0; i < messageIds.length; i += BATCH_MODIFY_SIZE) {
    const chunk = messageIds.slice(i, i + BATCH_MODIFY_SIZE);
    batchCount += 1;

    const ok = retryBatchModifyChunk(chunk, addLabelIds || [], removeLabelIds || [], batchCount);

    if (!ok) {
      failureCount += 1;
    }
  }

  return { batchCount, failureCount };
}

/**
 * Retry a single batchModify chunk using exponential backoff.
 *
 * @param {string[]} chunk Message IDs for this batch
 * @param {string[]} addLabelIds Label IDs to add
 * @param {string[]} removeLabelIds Label IDs to remove
 * @param {number} batchNumber Human-friendly batch number for logging
 * @returns {boolean} True on success, false if all attempts fail
 */
function retryBatchModifyChunk(chunk, addLabelIds, removeLabelIds, batchNumber) {
  let lastError = null;

  for (let attempt = 1; attempt <= RETRY_MAX_ATTEMPTS; attempt++) {
    try {
      Gmail.Users.Messages.batchModify(
        {
          ids: chunk,
          addLabelIds: addLabelIds,
          removeLabelIds: removeLabelIds,
        },
        "me"
      );

      if (attempt === 1) {
        logDebug(`batchModify success: batch ${batchNumber}, ${chunk.length} message(s).`);
      } else {
        logInfo(
          `batchModify recovered: batch ${batchNumber} succeeded on attempt ${attempt}/${RETRY_MAX_ATTEMPTS}.`
        );
      }

      return true;
    } catch (err) {
      lastError = err;
      const message = err && err.message ? err.message : String(err);

      if (attempt === RETRY_MAX_ATTEMPTS) {
        logInfo(
          `batchModify failed permanently for batch ${batchNumber} (${chunk.length} message(s)) ` +
          `after ${RETRY_MAX_ATTEMPTS} attempt(s): ${message}`
        );
        break;
      }

      const delayMs = RETRY_INITIAL_DELAY_MS * Math.pow(2, attempt - 1);
      logInfo(
        `batchModify failed for batch ${batchNumber} (${chunk.length} message(s)) on attempt ` +
        `${attempt}/${RETRY_MAX_ATTEMPTS}: ${message}. Retrying in ${delayMs} ms.`
      );
      Utilities.sleep(delayMs);
    }
  }

  logDebug(
    `Last error for batch ${batchNumber}: ${lastError && lastError.message ? lastError.message : lastError}`
  );

  return false;
}

/* ========================================================================== */
/* Continuation state and triggers                                             */
/* ========================================================================== */

/**
 * Load saved continuation state from Script Properties.
 *
 * @returns {Object|null} Parsed state object, or null if none/invalid
 */
function loadContinuationState() {
  const raw = PropertiesService.getScriptProperties().getProperty(STATE_KEY);
  if (!raw) {
    return null;
  }

  try {
    return JSON.parse(raw);
  } catch (err) {
    logInfo(`Could not parse continuation state. Starting over. Error: ${err.message}`);
    return null;
  }
}

/**
 * Save continuation state to Script Properties.
 *
 * @param {Object} state Serializable state object
 */
function saveContinuationState(state) {
  PropertiesService.getScriptProperties().setProperty(STATE_KEY, JSON.stringify(state));
}

/**
 * Remove saved continuation state.
 */
function clearContinuationState() {
  PropertiesService.getScriptProperties().deleteProperty(STATE_KEY);
}

/**
 * Delete any existing continuation triggers for this script.
 */
function deleteContinuationTriggers() {
  const triggers = ScriptApp.getProjectTriggers();

  for (const trigger of triggers) {
    if (trigger.getHandlerFunction() === CONTINUATION_FUNCTION_NAME) {
      ScriptApp.deleteTrigger(trigger);
    }
  }
}

/**
 * Schedule a time-based continuation trigger.
 */
function scheduleContinuationTrigger() {
  deleteContinuationTriggers();

  ScriptApp.newTrigger(CONTINUATION_FUNCTION_NAME)
    .timeBased()
    .after(CONTINUATION_DELAY_MS)
    .create();

  logInfo(`Continuation trigger scheduled in ${Math.round(CONTINUATION_DELAY_MS / 1000)} second(s).`);
}

/**
 * Build state object pointing to the current label/ancestor/page position.
 *
 * @param {string} labelName Current label name
 * @param {string} ancestorName Current ancestor name
 * @param {number} labelIndex Current label index
 * @param {number} ancestorIndex Current ancestor index
 * @param {string|null} pageToken Gmail page token for continuation
 * @returns {Object} Continuation state
 */
function buildContinuationState(labelName, ancestorName, labelIndex, ancestorIndex, pageToken) {
  return {
    labelName,
    ancestorName,
    labelIndex,
    ancestorIndex,
    pageToken: pageToken || null,
    savedAt: new Date().toISOString(),
  };
}

/**
 * Resolve the resume position from saved state.
 *
 * @param {Object|null} savedState Previously saved state
 * @param {Object[]} eligibleLabels Sorted eligible labels
 * @returns {{labelIndex:number, ancestorIndex:number, pageToken:(string|null)}}
 */
function getStartPosition(savedState, eligibleLabels) {
  if (!savedState) {
    return { labelIndex: 0, ancestorIndex: 0, pageToken: null };
  }

  const labelIndexByName = eligibleLabels.findIndex(label => label.name === savedState.labelName);

  if (labelIndexByName === -1) {
    logInfo(`Saved resume label "${savedState.labelName}" no longer exists or is no longer eligible. Starting over.`);
    return { labelIndex: 0, ancestorIndex: 0, pageToken: null };
  }

  const labelName = eligibleLabels[labelIndexByName].name;
  const ancestorNames = getAncestorNames(labelName);
  const ancestorIndexByName = ancestorNames.findIndex(name => name === savedState.ancestorName);

  if (ancestorIndexByName === -1) {
    logInfo(`Saved resume ancestor "${savedState.ancestorName}" no longer applies to "${labelName}". Starting label from first ancestor.`);
    return { labelIndex: labelIndexByName, ancestorIndex: 0, pageToken: null };
  }

  return {
    labelIndex: labelIndexByName,
    ancestorIndex: ancestorIndexByName,
    pageToken: savedState.pageToken || null,
  };
}

/* ========================================================================== */
/* Validation and formatting                                                   */
/* ========================================================================== */

/**
 * Validate a skip list against current Gmail labels.
 *
 * @param {string} listName Human-readable list name
 * @param {string[]} skipList List values to validate
 * @param {Set<string>} allLabelNameSet All Gmail label names
 * @param {Set<string>} systemLabelNameSet System label names
 */
function validateSkipList(listName, skipList, allLabelNameSet, systemLabelNameSet) {
  const missing = skipList.filter(name => !allLabelNameSet.has(name));
  const system = skipList.filter(name => systemLabelNameSet.has(name));

  let message = `${skipList.length} entr${skipList.length === 1 ? "y" : "ies"} in ${listName} skip list`;

  if (missing.length > 0) {
    message += `\nMissing labels (${missing.length}): [ ${missing.join(", ")} ]`;
  }

  if (system.length > 0) {
    message += `\nSystem labels ignored (${system.length}): [ ${system.join(", ")} ]`;
  }

  logInfo(message);
}

/**
 * Format skip-list matches for verbose output.
 *
 * @param {{label:string, reason:string}[]} skipMatches Matched labels with reasons
 * @returns {string} Formatted multi-line log text
 */
function formatSkipMatches(skipMatches) {
  const width = getPadding(skipMatches.map(item => item.label), 2);

  return [
    "Gmail user labels that matched a skip list:",
    ...skipMatches.map((item, index) => {
      const num = String(index + 1).padStart(3, " ");
      const label = item.label.padEnd(width, " ");
      return `${num} ${label}${item.reason}`;
    }),
  ].join("\n");
}

/**
 * Build a preview log of message IDs touched for one label/ancestor pair.
 *
 * @param {string} labelName Child label
 * @param {string} ancestorName Ancestor label added
 * @param {string[]} messageIds Updated message IDs
 * @returns {string} Formatted preview text
 */
function buildMessagePreview(labelName, ancestorName, messageIds) {
  const previewCount = Math.min(messageIds.length, 20);
  const previewLines = messageIds
    .slice(0, previewCount)
    .map(id => `  - ${id}`);

  if (messageIds.length > previewCount) {
    previewLines.push(`  ...and ${messageIds.length - previewCount} more`);
  }

  return [
    `Updated for "${labelName}" -> "${ancestorName}":`,
    ...previewLines,
  ].join("\n");
}

/**
 * Return width needed to align a column of strings.
 *
 * @param {Array<*>} values Values to measure
 * @param {number} extra Additional padding
 * @returns {number} Computed width
 */
function getPadding(values, extra) {
  if (!Array.isArray(values) || values.length === 0) {
    return extra || 1;
  }

  const longest = values.reduce((max, value) => {
    return Math.max(max, String(value).length);
  }, 0);

  return longest + (extra || 1);
}

/* ========================================================================== */
/* Logging                                                                     */
/* ========================================================================== */

/**
 * Log an info-level message.
 *
 * @param {string} message Message text
 */
function logInfo(message) {
  Logger.log(message);
}

/**
 * Log a verbose-level message if LOG_LEVEL >= 2.
 *
 * @param {string} message Message text
 */
function logVerbose(message) {
  if (normalizeLogLevel(LOG_LEVEL) >= 2) {
    Logger.log(message);
  }
}

/**
 * Log a debug-level message if LOG_LEVEL >= 3.
 *
 * @param {string} message Message text
 */
function logDebug(message) {
  if (normalizeLogLevel(LOG_LEVEL) >= 3) {
    Logger.log(message);
  }
}
