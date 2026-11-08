"use strict";

/**
 * StatePact Concurrency Playground — browser simulation.
 *
 * This file mirrors the commit-validation rules of StatePact so the static
 * Space can run without a Python server. It is deliberately small and explicit:
 *
 *   none          no validation
 *   optimistic    validate read set + write base versions
 *   snapshot      validate write base versions only (write skew is possible)
 *   serializable  optimistic behaviour for this workload
 *   pessimistic   strict 2PL over the scenario keys
 *
 * The Python implementation is the source of truth:
 * https://github.com/mithun-27/StatePact
 */

const MODES = ["none", "optimistic", "snapshot", "serializable", "pessimistic"];
const MAX_ATTEMPTS = 200;

function mulberry32(seed) {
  let a = seed >>> 0;
  return function random() {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

class Store {
  constructor(initial) {
    this.values = { ...initial };
    this.versions = {};
    for (const key of Object.keys(initial)) {
      this.versions[key] = 0;
    }
  }

  version(key) {
    return this.versions[key] ?? 0;
  }

  snapshot(keys) {
    const values = {};
    const versions = {};
    for (const key of keys) {
      values[key] = this.values[key];
      versions[key] = this.version(key);
    }
    return { values, versions };
  }

  apply(writes) {
    for (const [key, value] of Object.entries(writes)) {
      this.values[key] = value;
      this.versions[key] = this.version(key) + 1;
    }
  }
}

class LockTable {
  constructor() {
    this.locked = new Set();
  }

  async acquire(keys) {
    const ordered = [...keys].sort();
    for (let attempt = 0; attempt < 20000; attempt += 1) {
      if (ordered.every((key) => !this.locked.has(key))) {
        for (const key of ordered) {
          this.locked.add(key);
        }
        return true;
      }
      await sleep(1);
    }
    return false;
  }

  release(keys) {
    for (const key of keys) {
      this.locked.delete(key);
    }
  }
}

const SCENARIOS = {
  inventory: {
    title: "Inventory",
    keys: ["stock"],
    initial: (stock) => ({ stock }),
    decide(values) {
      if (values.stock <= 0) {
        return null;
      }
      return { writes: { stock: values.stock - 1 }, accepted: true };
    },
  },
  counter: {
    title: "Counter",
    keys: ["value"],
    initial: () => ({ value: 0 }),
    decide(values) {
      return { writes: { value: values.value + 1 }, accepted: true };
    },
  },
  write_skew: {
    title: "Write skew",
    keys: ["alice", "bob"],
    initial: () => ({ alice: true, bob: true }),
    decide(values, transaction) {
      if (!values.alice || !values.bob) {
        return null;
      }
      return { writes: { [transaction.role]: false }, accepted: true };
    },
  },
};

/**
 * Run one transaction attempt, including retries for validation conflicts.
 * Returns { status, accepted, conflicts, retries }.
 */
async function runAttempt(scenario, mode, store, locks, rng, transaction) {
  const think = () => sleep(2 + Math.floor(rng() * 7));

  if (mode === "pessimistic") {
    const keys = SCENARIOS[scenario].keys;
    const acquired = await locks.acquire(keys);
    if (!acquired) {
      return { status: "failed", accepted: false, conflicts: 1, retries: 0 };
    }
    try {
      const snapshot = store.snapshot(keys);
      const decision = SCENARIOS[scenario].decide(snapshot.values, transaction);
      if (!decision) {
        return { status: "aborted", accepted: false, conflicts: 0, retries: 0 };
      }
      await think();
      store.apply(decision.writes);
      return { status: "committed", accepted: decision.accepted, conflicts: 0, retries: 0 };
    } finally {
      locks.release(keys);
    }
  }

  const keys = SCENARIOS[scenario].keys;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
    const snapshot = store.snapshot(keys);
    const decision = SCENARIOS[scenario].decide(snapshot.values, transaction);
    if (!decision) {
      return { status: "aborted", accepted: false, conflicts: attempt - 1, retries: attempt - 1 };
    }

    const writes = decision.writes;
    const baseVersions = { ...snapshot.versions };
    for (const key of Object.keys(writes)) {
      baseVersions[key] = store.version(key);
    }

    await think();

    if (mode !== "none") {
      const validated =
        mode === "snapshot"
          ? Object.keys(writes)
          : [...new Set([...keys, ...Object.keys(writes)])];
      const conflict = validated.some((key) => store.version(key) !== baseVersions[key]);
      if (conflict) {
        if (attempt < MAX_ATTEMPTS) {
          continue;
        }
        return {
          status: "failed",
          accepted: false,
          conflicts: attempt,
          retries: attempt - 1,
        };
      }
    }

    store.apply(writes);
    return {
      status: "committed",
      accepted: decision.accepted,
      conflicts: attempt - 1,
      retries: attempt - 1,
    };
  }

  return { status: "failed", accepted: false, conflicts: MAX_ATTEMPTS, retries: MAX_ATTEMPTS };
}

async function runInventoryOrCounter(scenario, mode, agents, operations, stock, rng) {
  const config = SCENARIOS[scenario];
  const store = new Store(config.initial(stock));
  const locks = new LockTable();
  let accepted = 0;
  let conflicts = 0;
  let retries = 0;
  let failures = 0;

  async function worker(index) {
    for (let operation = 0; operation < operations; operation += 1) {
      const result = await runAttempt(
        scenario,
        mode,
        store,
        locks,
        rng,
        { agent: `agent-${index}`, operation },
      );
      conflicts += result.conflicts;
      retries += result.retries;
      if (result.status === "committed" && result.accepted) {
        accepted += 1;
      }
      if (result.status === "failed") {
        failures += 1;
      }
    }
  }

  await Promise.all(Array.from({ length: agents }, (_, index) => worker(index)));

  const expected = scenario === "inventory" ? Math.min(agents * operations, stock) : agents * operations;
  const actual = scenario === "inventory" ? store.values.stock : store.values.value;
  return {
    scenario,
    isolation: mode,
    agents,
    operations: agents * operations,
    initial_stock: stock,
    accepted,
    expected,
    actual,
    conflicts,
    retries,
    failures,
    violations: 0,
    oversold: scenario === "inventory" ? Math.max(0, accepted - stock) : 0,
    invariant:
      scenario === "inventory"
        ? `remaining stock must be >= 0 (initial ${stock}); accepted must be <= ${stock}`
        : `counter must equal ${agents * operations}`,
  };
}

async function runWriteSkew(mode, trials, rng) {
  let violations = 0;
  let conflicts = 0;
  let retries = 0;
  let failures = 0;

  for (let trial = 0; trial < trials; trial += 1) {
    const store = new Store(SCENARIOS.write_skew.initial());
    const locks = new LockTable();
    const [first, second] = await Promise.all([
      runAttempt("write_skew", mode, store, locks, rng, { role: "alice", trial }),
      runAttempt("write_skew", mode, store, locks, rng, { role: "bob", trial }),
    ]);

    for (const result of [first, second]) {
      conflicts += result.conflicts;
      retries += result.retries;
      if (result.status === "failed") {
        failures += 1;
      }
    }

    if (store.values.alice === false && store.values.bob === false) {
      violations += 1;
    }
  }

  return {
    scenario: "write_skew",
    isolation: mode,
    agents: 2,
    operations: trials,
    accepted: trials - conflicts,
    expected: 0,
    actual: violations,
    conflicts,
    retries,
    failures,
    violations,
    oversold: 0,
    invariant: "at least one doctor must remain on call in every trial",
  };
}

async function runExperiment({ scenario, isolation, agents, operations, stock = 10, seed = Date.now() }) {
  if (!MODES.includes(isolation)) {
    throw new Error(`Unknown isolation mode: ${isolation}`);
  }
  if (!SCENARIOS[scenario]) {
    throw new Error(`Unknown scenario: ${scenario}`);
  }

  const rng = mulberry32(seed);
  const started = performance.now();
  const result =
    scenario === "write_skew"
      ? await runWriteSkew(isolation, operations, rng)
      : await runInventoryOrCounter(scenario, isolation, agents, operations, stock, rng);
  result.duration_seconds = (performance.now() - started) / 1000;
  result.seed = seed;
  return result;
}

/* ------------------------------------------------------------------------- */
/* Browser UI                                                                */
/* ------------------------------------------------------------------------- */

if (typeof document !== "undefined") {
  const $ = (id) => document.getElementById(id);

  const MODE_HINTS = {
    none: "No validation. Unsafe by design — a baseline.",
    optimistic: "Snapshot reads plus full read/write validation.",
    snapshot: "First-committer-wins. Write skew is possible.",
    serializable: "Optimistic validation plus a graph audit.",
    pessimistic: "Strict 2PL: waits for locks instead of retrying.",
  };

  const SCENARIO_HINTS = {
    inventory: "Concurrent reservations compete for a fixed number of units.",
    counter: "Concurrent increments race on one counter key.",
    write_skew: "Two transactions each write a different key after reading both.",
  };

  const controls = $("controls");
  const scenarioInput = $("scenario");
  const isolationInput = $("isolation");
  const agentsInput = $("agents");
  const operationsInput = $("operations");
  const stockInput = $("stock");
  const runButton = $("run");
  const status = $("run-status");

  let runNumber = 0;

  function formatSeconds(value) {
    return `${value.toFixed(3)}s`;
  }

  function updateControlLabels() {
    $("agents-value").textContent = agentsInput.value;
    $("operations-value").textContent = operationsInput.value;
    $("stock-value").textContent = stockInput.value;

    const skew = scenarioInput.value === "write_skew";
    $("operations-label").textContent = skew ? "Trials" : "Operations per agent";
    $("operations-hint").textContent = skew
      ? "Each trial races two transactions."
      : "Each operation is a separate transaction.";
    stockInput.disabled = scenarioInput.value !== "inventory";
    $("scenario-hint").textContent = SCENARIO_HINTS[scenarioInput.value];
    $("isolation-hint").textContent = MODE_HINTS[isolationInput.value];
  }

  function verdictFor(result) {
    if (result.scenario === "inventory") {
      if (result.oversold > 0) return { tone: "bad", label: "Oversold" };
      if (result.isolation === "none") return { tone: "warn", label: "Unsafe baseline" };
      if (result.accepted > result.expected) return { tone: "warn", label: "Check" };
      return { tone: "good", label: "Invariant held" };
    }
    if (result.scenario === "counter") {
      const lost = Math.max(0, result.expected - result.actual);
      return lost === 0
        ? { tone: "good", label: "Exact" }
        : { tone: "bad", label: `${lost} lost` };
    }
    return result.violations === 0
      ? { tone: "good", label: "Invariant held" }
      : { tone: "bad", label: `${result.violations} violated` };
  }

  function headlineFor(result) {
    if (result.scenario === "inventory") {
      let headline = `Accepted ${result.accepted} reservation${result.accepted === 1 ? "" : "s"} against ${result.initial_stock} units of stock; ${result.actual} remained.`;
      if (result.oversold > 0) {
        headline += ` ${result.oversold} unit${result.oversold === 1 ? "" : "s"} oversold.`;
      } else if (result.accepted > result.expected) {
        headline += " More reservations were accepted than the stock allows.";
      }
      return headline;
    }
    if (result.scenario === "counter") {
      const lost = Math.max(0, result.expected - result.actual);
      return `${result.actual} of ${result.expected} increments survived; ${lost} lost update${lost === 1 ? "" : "s"}.`;
    }
    return `${result.violations} of ${result.operations} trials left no doctor on call.`;
  }

  function render(result) {
    const verdict = verdictFor(result);
    const verdictEl = $("verdict");
    verdictEl.dataset.tone = verdict.tone;
    verdictEl.textContent = verdict.label;

    $("result-eyebrow").textContent = `${SCENARIOS[result.scenario].title} · ${result.isolation}`;
    $("headline").textContent = headlineFor(result);

    $("stat-accepted").textContent = result.accepted;
    $("stat-expected").textContent = result.expected;
    $("stat-actual").textContent = result.actual;
    $("stat-conflicts").textContent = result.conflicts;
    $("stat-retries").textContent = result.retries;
    $("stat-failures").textContent = result.failures;
    $("stat-duration").textContent = formatSeconds(result.duration_seconds);

    const extraLabel = $("stat-extra-label");
    const extraValue = $("stat-extra");
    if (result.scenario === "inventory") {
      extraLabel.textContent = "Oversold";
      extraValue.textContent = result.oversold;
    } else if (result.scenario === "counter") {
      extraLabel.textContent = "Lost updates";
      extraValue.textContent = Math.max(0, result.expected - result.actual);
    } else {
      extraLabel.textContent = "Violations";
      extraValue.textContent = result.violations;
    }

    const cells = [
      result.scenario,
      result.isolation,
      result.agents,
      result.operations,
      result.accepted,
      result.expected,
      result.actual,
      result.conflicts,
      result.retries,
      result.failures,
      result.duration_seconds.toFixed(3),
      result.violations,
      result.oversold,
    ];

    $("result-row").innerHTML = `<tr>${cells.map((cell) => `<td>${cell}</td>`).join("")}</tr>`;
    $("invariant").textContent = `Invariant: ${result.invariant}`;
    status.textContent = `Run ${runNumber} complete · seed ${result.seed}.`;
  }

  async function run() {
    const scenario = scenarioInput.value;
    const isolation = isolationInput.value;
    const agents = Number(agentsInput.value);
    const operations = Number(operationsInput.value);
    const stock = Number(stockInput.value);

    runNumber += 1;
    runButton.disabled = true;
    runButton.dataset.running = "true";
    runButton.querySelector(".button-label").textContent = "Running…";
    status.textContent = `Running ${scenario} under ${isolation}…`;
    $("headline").textContent = "Running experiment…";
    $("verdict").dataset.tone = "neutral";
    $("verdict").textContent = "…";

    // Yield once so the busy state paints before the simulation starts.
    await sleep(30);

    try {
      const result = await runExperiment({
        scenario,
        isolation,
        agents,
        operations,
        stock,
        seed: (Date.now() ^ (runNumber * 2654435761)) >>> 0,
      });
      render(result);
    } catch (error) {
      status.textContent = `Run failed: ${error.message}`;
      $("headline").textContent = "The experiment could not complete.";
      $("verdict").dataset.tone = "bad";
      $("verdict").textContent = "Error";
    } finally {
      runButton.disabled = false;
      runButton.dataset.running = "false";
      runButton.querySelector(".button-label").textContent = "Run experiment";
    }
  }

  controls.addEventListener("submit", (event) => {
    event.preventDefault();
    void run();
  });

  for (const input of [scenarioInput, isolationInput, agentsInput, operationsInput, stockInput]) {
    input.addEventListener("input", updateControlLabels);
    input.addEventListener("change", updateControlLabels);
  }

  updateControlLabels();
  void run();
}

/**
 * Hugging Face renders this playground inside an iframe. github.com answers
 * with `X-Frame-Options: deny` and `frame-ancestors 'none'`, so if the preview
 * iframe ever navigates to an off-site URL the whole playground is replaced by
 * "github.com refused to connect". Force every off-site link to open in a new
 * tab so the preview always keeps running this page.
 */
function installExternalLinkGuard() {
  if (typeof document === "undefined" || typeof window === "undefined") {
    return;
  }

  document.addEventListener(
    "click",
    (event) => {
      if (event.defaultPrevented || event.button !== 0) {
        return;
      }
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
        return;
      }
      const target = event.target;
      const anchor = target instanceof Element ? target.closest("a[href]") : null;
      if (!anchor) {
        return;
      }
      const href = anchor.getAttribute("href");
      if (!href || href.startsWith("#")) {
        return;
      }
      let url;
      try {
        url = new URL(href, window.location.href);
      } catch {
        return;
      }
      if (url.origin === window.location.origin) {
        return;
      }
      event.preventDefault();
      const opened = window.open(url.href, "_blank", "noopener,noreferrer");
      if (opened) {
        opened.opener = null;
      }
    },
    true,
  );
}

installExternalLinkGuard();

if (typeof module !== "undefined" && module.exports) {
  module.exports = { runExperiment, MODES };
}
