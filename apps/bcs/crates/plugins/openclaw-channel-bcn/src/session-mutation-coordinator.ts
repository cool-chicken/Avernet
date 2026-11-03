export type SessionCoordinatorKey = {
  botId: string;
  canonicalSessionKey: string;
};

export type BcsRunIdentity = SessionCoordinatorKey & {
  bcsRunId: string;
  agentRunId?: string;
};

export type InjectAdmissionResult =
  | { status: 'ready' }
  | { status: 'aborted'; reason: string }
  | { status: 'timed_out'; reason: string }
  | { status: 'gateway_restarted'; reason: string };

type Owner = {
  identity: BcsRunIdentity;
  generation: number;
  state: 'pending' | 'active' | 'aborting' | 'timed_out' | 'disconnected';
  waiters: Set<Waiter>;
};

type Waiter = {
  resolve: (result: InjectAdmissionResult) => void;
  timer: ReturnType<typeof setTimeout>;
};

export class SessionCoordinatorBusyError extends Error {
  readonly code = 'SESSION_BUSY';
  readonly retryable = false;

  constructor(key: SessionCoordinatorKey) {
    super(`A BCS Agent run is already active for ${key.botId}/${key.canonicalSessionKey}`);
    this.name = 'SessionCoordinatorBusyError';
  }
}

function keyOf(key: SessionCoordinatorKey): string {
  return `${key.botId}\u0000${key.canonicalSessionKey}`;
}

function matches(owner: Owner, identity: BcsRunIdentity): boolean {
  return owner.identity.botId === identity.botId
    && owner.identity.canonicalSessionKey === identity.canonicalSessionKey
    && owner.identity.bcsRunId === identity.bcsRunId
    && owner.identity.agentRunId === identity.agentRunId;
}

function matchesForBinding(owner: Owner, identity: BcsRunIdentity): boolean {
  return owner.identity.botId === identity.botId
    && owner.identity.canonicalSessionKey === identity.canonicalSessionKey
    && owner.identity.bcsRunId === identity.bcsRunId
    && (
      owner.identity.agentRunId === undefined
      || owner.identity.agentRunId === identity.agentRunId
    );
}

export class SessionMutationCoordinator {
  private readonly owners = new Map<string, Owner>();
  private nextGeneration = 0;

  reserve(identity: BcsRunIdentity): number {
    const key = keyOf(identity);
    if (this.owners.has(key)) {
      throw new SessionCoordinatorBusyError(identity);
    }
    const owner: Owner = {
      identity: { ...identity },
      generation: ++this.nextGeneration,
      state: 'pending',
      waiters: new Set(),
    };
    this.owners.set(key, owner);
    return owner.generation;
  }

  bindAgentRun(identity: BcsRunIdentity): boolean {
    const owner = this.owners.get(keyOf(identity));
    if (!owner || !matchesForBinding(owner, identity)) return false;
    if (owner.identity.agentRunId && owner.identity.agentRunId !== identity.agentRunId) return false;
    if (!identity.agentRunId) return false;
    owner.identity.agentRunId = identity.agentRunId;
    owner.state = 'active';
    return true;
  }

  rekey(identity: BcsRunIdentity, nextKey: SessionCoordinatorKey): boolean {
    const currentKey = keyOf(identity);
    const owner = this.owners.get(currentKey);
    if (!owner || !matches(owner, identity)) return false;
    const nextKeyValue = keyOf(nextKey);
    const existing = this.owners.get(nextKeyValue);
    if (existing && existing !== owner) {
      throw new SessionCoordinatorBusyError(nextKey);
    }
    this.owners.delete(currentKey);
    owner.identity.botId = nextKey.botId;
    owner.identity.canonicalSessionKey = nextKey.canonicalSessionKey;
    this.owners.set(nextKeyValue, owner);
    return true;
  }

  waitForInject(
    key: SessionCoordinatorKey,
    timeoutMs: number,
  ): Promise<InjectAdmissionResult> {
    const owner = this.owners.get(keyOf(key));
    if (!owner) return Promise.resolve({ status: 'ready' });
    if (owner.state === 'disconnected') {
      return Promise.resolve({
        status: 'gateway_restarted',
        reason: 'OpenClaw gateway disconnected before the BCS Agent run completed',
      });
    }
    if (owner.state === 'timed_out') {
      return Promise.resolve({
        status: 'timed_out',
        reason: 'BCS Agent run did not reach a terminal state; Session remains locked',
      });
    }

    return new Promise(resolve => {
      const waiter: Waiter = {
        resolve,
        timer: setTimeout(() => {
          owner.waiters.delete(waiter);
          resolve({ status: 'timed_out', reason: 'BCS Agent run did not reach a terminal state' });
        }, Math.max(1, timeoutMs)),
      };
      owner.waiters.add(waiter);
    });
  }

  abort(identity: BcsRunIdentity): boolean {
    const owner = this.owners.get(keyOf(identity));
    if (!owner || !matches(owner, identity)) return false;
    owner.state = 'aborting';
    return true;
  }

  markTimedOut(identity: BcsRunIdentity): boolean {
    const owner = this.owners.get(keyOf(identity));
    if (!owner || !matches(owner, identity)) return false;
    owner.state = 'timed_out';
    this.resolveWaiters(owner, {
      status: 'timed_out',
      reason: 'BCS Agent run did not reach a terminal state; Session remains locked',
    });
    return true;
  }

  complete(identity: BcsRunIdentity, terminalConfirmed = false): boolean {
    const owner = this.owners.get(keyOf(identity));
    if (!owner || !matches(owner, identity)) return false;
    if (!terminalConfirmed) return false;
    this.owners.delete(keyOf(identity));
    this.resolveWaiters(owner, { status: 'ready' });
    return true;
  }

  restart(botId?: string): void {
    for (const [key, owner] of this.owners) {
      if (botId && owner.identity.botId !== botId) continue;
      owner.state = 'disconnected';
      this.resolveWaiters(owner, {
        status: 'gateway_restarted',
        reason: 'OpenClaw gateway restarted before the BCS Agent run completed',
      });
    }
  }

  hasOwner(key: SessionCoordinatorKey): boolean {
    return this.owners.has(keyOf(key));
  }

  getGeneration(key: SessionCoordinatorKey): number | undefined {
    return this.owners.get(keyOf(key))?.generation;
  }

  private resolveWaiters(owner: Owner, result: InjectAdmissionResult): void {
    for (const waiter of owner.waiters) {
      clearTimeout(waiter.timer);
      waiter.resolve(result);
    }
    owner.waiters.clear();
  }
}
