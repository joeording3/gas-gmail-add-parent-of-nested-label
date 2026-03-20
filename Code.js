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
 * - Uses batchModify for performance
 * - Chunks batchModify to Gmail API's 1000-ID limit
 * - Retries failed batchModify chunks with exponential backoff
 * - Catches per-batch errors so one failed chunk does not stop the whole run
 * - Stops after MAX_MESSAGES_PER_RUN to reduce Apps Script timeout risk
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
 * - Continuation resumes at the next label/ancestor pair, not mid-query page.
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
 * Notes:
 * - These values should use valid Gmail search syntax labels/operators.
 * - Examples:
 *     "UNREAD"
 *     "STARRED"
 *     "IMPORTANT"
 *     "INBOX"
 *     "SENT"
 *     "DRAFT"
 *     "SPAM"
 *     "TRASH"
 *     "CATEGORY_PERSONAL"
 *     "CATEGORY_SOCIAL"
 *     "CATEGORY_PROMOTIONS"
 *     "CATEGORY_UPDATES"
 *     "CATEGORY_FORUMS"
 *
 * Behavior:
 * - REQUIRED_SYSTEM_LABELS_ANY:
 *     At least one must match. Combined as: (label:X OR label:Y ...)
 * - REQUIRED_SYSTEM_LABELS_ALL:
 *     All must match. Combined as: label:X label:Y ...
 * - EXCLUDED_SYSTEM_LABELS:
 *     None may match. Combined as: -label:X -label:Y ...
 *
 * Example:
 *   Only unread inbox mail:
 *     REQUIRED_SYSTEM_LABELS_ALL = ["UNREAD", "INBOX"]
 *
 *   Only unread or starred mail:
 *     REQUIRED_SYSTEM_LABELS_ANY = ["UNREAD", "STARRED"]
 *
 *   Skip trash and spam:
 *     EXCLUDED_SYSTEM_LABELS = ["TRASH", "SPAM"]
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
 * Once this many messages have been scheduled for update in a run, the script
 * stops and schedules a continuation trigger so the next execution can continue.
 */
const MAX_MESSAGES_PER_RUN = 10000;

/**
 * Delay before continuation trigger fires, in milliseconds.
 */
const CONTINUATION_DELAY_MS = 60 * 1000;

/**
 * Retry configuration for failed batchModify chunks.
 *
 * The script retries transient failures with exponential backoff:
 *   delay = RETRY_INITIAL_DELAY_MS * 2^(attempt - 1)
 *
 * Example with defaults:
 *   attempt 1 retry -> 1 second
 *   attempt 2 retry -> 2 seconds
 *   attempt 3 retry -> 4 seconds
 */
const RETRY_MAX_ATTEMPTS = 4;
const RETRY_INITIAL_DELAY_MS = 1000;

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
 * Called by a time-based trigger when a prior run hit the per-run cap and
 * scheduled a continuation.
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
 * Workflow:
 * 1. Load Gmail labels
 * 2. Validate skip lists
 * 3. Validate system-label filters
 * 4. Build eligible nested user labels
 * 5. Walk each label and each ancestor
 * 6. Find messages that have child label but not ancestor label
 * 7. Optionally restrict by system-label filters
 * 8. Add missing ancestor label in batches of up to 1000
 * 9. Stop early if MAX_MESSAGES_PER_RUN is reached
 * 10. Save continuation state and schedule next trigger if needed
 *
 * @param {Object} options
 * @param {boolean} options.resume Whether this run is resuming from saved state
 */
function runAddParentLabel(options) {
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
        `Resuming from label index ${startPosition.labelIndex + 1}, ancestor index ${startPosition.ancestorIndex + 1}.`
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
  let hitRunCap = false;
  let nextState = null;

  for (let labelIndex = startPosition.labelIndex; labelIndex < eligibleLabels.length; labelIndex++) {
    const label = eligibleLabels[labelIndex];
    labelsChecked += 1;

    const ancestorNames = getAncestorNames(label.name);
    if (ancestorNames.length === 0) {
      continue;
    }

    if (logLevel >= 3) {
      logDebug(`Checking "${label.name}" with ancestors: ${ancestorNames.join(", ")}`);
    }

    const ancestorStartIndex =
      labelIndex === startPosition.labelIndex ? startPosition.ancestorIndex : 0;

    for (let ancestorIndex = ancestorStartIndex; ancestorIndex < ancestorNames.length; ancestorIndex++) {
      const ancestorName = ancestorNames[ancestorIndex];
      const ancestorLabel = labelMap.get(ancestorName);

      if (!ancestorLabel) {
        logInfo(`Skipping "${label.name}" -> missing ancestor label "${ancestorName}".`);
        continue;
      }

      const query = buildMessageSearchQuery(label.name, ancestorName, systemFilterQuery);
      const messageIds = listAllMessageIds(query);

      if (messageIds.length === 0) {
        if (logLevel >= 3) {
          logDebug(`No messages missing "${ancestorName}" for "${label.name}".`);
        }
        continue;
      }

      if (
        totalMessagesTouched > 0 &&
        totalMessagesTouched + messageIds.length > MAX_MESSAGES_PER_RUN
      ) {
        hitRunCap = true;
        nextState = buildContinuationState(label.name, ancestorName, labelIndex, ancestorIndex);
        logInfo(
          `Per-run cap reached before processing "${label.name}" -> "${ancestorName}". Scheduling continuation.`
        );
        break;
      }

      if (DRY_RUN) {
        logInfo(`[DRY RUN] Would add "${ancestorName}" to ${messageIds.length} message(s) labeled "${label.name}".`);
        totalMessagesTouched += messageIds.length;
        updateGroups += 1;
      } else {
        const result = batchModifyMessagesSafe(messageIds, [ancestorLabel.id], []);
        totalBatchCalls += result.batchCount;
        totalBatchFailures += result.failureCount;
        totalMessagesTouched += messageIds.length;
        updateGroups += 1;

        logInfo(
          `Added "${ancestorName}" to ${messageIds.length} message(s) labeled "${label.name}" ` +
          `in ${result.batchCount} batch(es)` +
          `${result.failureCount > 0 ? ` with ${result.failureCount} failed batch(es)` : ""}.`
        );
      }

      if (logLevel >= 2) {
        logVerbose(buildMessagePreview(label.name, ancestorName, messageIds));
      }

      if (totalMessagesTouched >= MAX_MESSAGES_PER_RUN) {
        hitRunCap = true;
        nextState = buildNextPositionAfterCurrent(eligibleLabels, labelIndex, ancestorNames, ancestorIndex);
        logInfo("Per-run cap reached. Scheduling continuation.");
        break;
      }
    }

    if (hitRunCap) {
      break;
    }
  }

  if (hitRunCap && nextState) {
    saveContinuationState(nextState);
    scheduleContinuationTrigger();
  } else {
    clearContinuationState();
    deleteContinuationTriggers();
  }

  if (totalMessagesTouched === 0 && !hitRunCap) {
    logInfo("All labeled messages already included the labels of their ancestors.");
    return;
  }

  logInfo(
    `${DRY_RUN ? "[DRY RUN] " : ""}Done. Checked ${labelsChecked} label(s), ` +
    `performed ${updateGroups} update group(s), touched ${totalMessagesTouched} message(s)` +
    `${DRY_RUN ? "." : `, across ${totalBatchCalls} batchModify call(s), with ${totalBatchFailures} failed batch(es).`}`
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
 * Examples of output:
 *   label:"UNREAD" label:"INBOX" -label:"TRASH" -label:"SPAM"
 *   {label:"UNREAD" label:"STARRED"} -label:"TRASH"
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
 * This is helpful for catching typos, though some Gmail search operators may
 * still be valid even if they do not appear in label listings exactly as typed.
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
 * Rules:
 * - Exact match in LABEL_SKIP_LIST => skip
 * - Descendant of any OFFSPRING_SKIP_LIST entry => skip
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
/* Gmail message search and modify                                             */
/* ========================================================================== */

/**
 * Return all message IDs matching a Gmail search query.
 *
 * Uses Gmail.Users.Messages.list with pagination until all result pages
 * have been fetched.
 *
 * @param {string} query Gmail search query
 * @returns {string[]} Matching message IDs
 */
function listAllMessageIds(query) {
  const ids = [];
  let pageToken = null;

  do {
    const response = Gmail.Users.Messages.list("me", {
      q: query,
      pageToken: pageToken,
      maxResults: SEARCH_PAGE_SIZE,
    });

    const messages = response.messages || [];
    ids.push(...messages.map(message => message.id));
    pageToken = response.nextPageToken || null;
  } while (pageToken);

  return ids;
}

/**
 * Add/remove labels in Gmail batchModify calls, chunked to Gmail's 1000-ID limit.
 *
 * This safe version catches errors per chunk so one failed chunk does not stop
 * the rest of the run, and retries each failed chunk using exponential backoff.
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
 * Attempts up to RETRY_MAX_ATTEMPTS times total.
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

      const isLastAttempt = attempt === RETRY_MAX_ATTEMPTS;
      const message = err && err.message ? err.message : String(err);

      if (isLastAttempt) {
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
 *
 * This prevents multiple overlapping time-based triggers from piling up.
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
 *
 * Existing continuation triggers are deleted first so only one continuation
 * trigger remains active at a time.
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
 * Build state object pointing to the current label/ancestor pair.
 *
 * Used when a run stops before processing the current pair.
 *
 * @param {string} labelName Current label name
 * @param {string} ancestorName Current ancestor name
 * @param {number} labelIndex Current label index
 * @param {number} ancestorIndex Current ancestor index
 * @returns {Object} Continuation state
 */
function buildContinuationState(labelName, ancestorName, labelIndex, ancestorIndex) {
  return {
    labelName,
    ancestorName,
    labelIndex,
    ancestorIndex,
    savedAt: new Date().toISOString(),
  };
}

/**
 * Build continuation state for the next logical position after finishing
 * the current label/ancestor pair.
 *
 * @param {Object[]} eligibleLabels Eligible label objects
 * @param {number} labelIndex Current label index
 * @param {string[]} ancestorNames Ancestors for current label
 * @param {number} ancestorIndex Current ancestor index
 * @returns {Object|null} Continuation state, or null if all work is complete
 */
function buildNextPositionAfterCurrent(eligibleLabels, labelIndex, ancestorNames, ancestorIndex) {
  if (ancestorIndex + 1 < ancestorNames.length) {
    return {
      labelName: eligibleLabels[labelIndex].name,
      ancestorName: ancestorNames[ancestorIndex + 1],
      labelIndex: labelIndex,
      ancestorIndex: ancestorIndex + 1,
      savedAt: new Date().toISOString(),
    };
  }

  if (labelIndex + 1 < eligibleLabels.length) {
    const nextLabelName = eligibleLabels[labelIndex + 1].name;
    const nextAncestors = getAncestorNames(nextLabelName);

    if (nextAncestors.length > 0) {
      return {
        labelName: nextLabelName,
        ancestorName: nextAncestors[0],
        labelIndex: labelIndex + 1,
        ancestorIndex: 0,
        savedAt: new Date().toISOString(),
      };
    }
  }

  return null;
}

/**
 * Resolve the resume position from saved state.
 *
 * Uses names when possible for resilience across label-order changes, and
 * falls back to numeric indices when needed.
 *
 * @param {Object|null} savedState Previously saved state
 * @param {Object[]} eligibleLabels Sorted eligible labels
 * @returns {{labelIndex:number, ancestorIndex:number}}
 */
function getStartPosition(savedState, eligibleLabels) {
  if (!savedState) {
    return { labelIndex: 0, ancestorIndex: 0 };
  }

  const labelIndexByName = eligibleLabels.findIndex(label => label.name === savedState.labelName);

  if (labelIndexByName === -1) {
    logInfo(`Saved resume label "${savedState.labelName}" no longer exists or is no longer eligible. Starting over.`);
    return { labelIndex: 0, ancestorIndex: 0 };
  }

  const labelName = eligibleLabels[labelIndexByName].name;
  const ancestorNames = getAncestorNames(labelName);
  const ancestorIndexByName = ancestorNames.findIndex(name => name === savedState.ancestorName);

  if (ancestorIndexByName === -1) {
    logInfo(`Saved resume ancestor "${savedState.ancestorName}" no longer applies to "${labelName}". Starting label from first ancestor.`);
    return { labelIndex: labelIndexByName, ancestorIndex: 0 };
  }

  return {
    labelIndex: labelIndexByName,
    ancestorIndex: ancestorIndexByName,
  };
}

/* ========================================================================== */
/* Validation and formatting                                                   */
/* ========================================================================== */

/**
 * Validate a skip list against current Gmail labels.
 *
 * Logs:
 * - number of entries
 * - labels that do not exist
 * - labels that are system labels
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
