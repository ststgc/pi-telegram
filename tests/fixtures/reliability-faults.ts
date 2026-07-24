/**
 * Deterministic reliability fault injection for crash-boundary tests
 * Zones: test infrastructure, recovery, fault injection
 * Owns the bounded test-only fault inventory and one-shot controller; this fixture
 * is outside package files and has no production registration or side effects.
 */

export const RELIABILITY_FAULT_IDS = [
  "IN-01",
  "IN-02",
  "IN-03",
  "IN-04",
  "IN-05",
  "IN-06",
  "IN-07",
  "IN-08",
  "IN-GROUP-01",
  "IN-GROUP-02",
  "IN-GROUP-03",
  "OUT-01",
  "OUT-02",
  "OUT-03",
  "OUT-04",
  "OUT-05",
  "OUT-06",
  "BUS-01",
  "BUS-02",
  "BUS-03",
  "BUS-04",
  "PAIR-01",
  "PAIR-02",
  "DOWN-01",
  "DOWN-02",
] as const;

export type ReliabilityFaultId = (typeof RELIABILITY_FAULT_IDS)[number];

const RELIABILITY_FAULT_ID_SET: ReadonlySet<string> = new Set(
  RELIABILITY_FAULT_IDS,
);

export class InjectedReliabilityFaultError extends Error {
  readonly faultId: ReliabilityFaultId;
  readonly seed: string;

  constructor(faultId: ReliabilityFaultId, seed: string) {
    super(
      `Injected reliability fault ${faultId} (PI_RELIABILITY_SEED=${seed})`,
    );
    this.name = "InjectedReliabilityFaultError";
    this.faultId = faultId;
    this.seed = seed;
  }
}

export interface ReliabilityFaultControllerOptions {
  seed?: string | number;
  env?: NodeJS.ProcessEnv;
}

export class ReliabilityFaultController {
  readonly selectedFaultId: ReliabilityFaultId;
  readonly seed: string;
  readonly hits: ReliabilityFaultId[] = [];
  #injected = false;

  constructor(
    selectedFaultId: ReliabilityFaultId,
    options: ReliabilityFaultControllerOptions = {},
  ) {
    if (!RELIABILITY_FAULT_ID_SET.has(selectedFaultId)) {
      throw new Error(`Unknown reliability fault id: ${String(selectedFaultId)}`);
    }
    const seed = options.seed ?? (options.env ?? process.env).PI_RELIABILITY_SEED;
    if (seed === undefined || String(seed).trim().length === 0) {
      throw new Error(
        "Reliability fault controller requires PI_RELIABILITY_SEED or an explicit seed",
      );
    }
    this.selectedFaultId = selectedFaultId;
    this.seed = String(seed);
  }

  hit(faultId: ReliabilityFaultId): void {
    if (!RELIABILITY_FAULT_ID_SET.has(faultId)) {
      throw new Error(`Unknown reliability fault id: ${String(faultId)}`);
    }
    this.hits.push(faultId);
    if (faultId === this.selectedFaultId && !this.#injected) {
      this.#injected = true;
      throw new InjectedReliabilityFaultError(faultId, this.seed);
    }
  }

  assertInjected(): void {
    if (!this.#injected) {
      throw new Error(
        `Reliability fault ${this.selectedFaultId} was not hit (PI_RELIABILITY_SEED=${this.seed})`,
      );
    }
  }

  get hasInjected(): boolean {
    return this.#injected;
  }
}
