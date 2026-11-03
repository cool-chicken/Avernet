import { strict as assert } from 'node:assert';
import {
  abortAllStreams,
  cleanupAgentEventsSubscription,
  handleChatAbort,
  handleChatSend,
  initAgentEventsSubscription,
} from '../src/inbound-handler.js';
import { setBcsRuntime } from '../src/index.js';
import { SessionMutationCoordinator } from '../src/session-mutation-coordinator.js';
import type { ResolvedBcsAccount } from '../src/types.js';

const account: ResolvedBcsAccount = {
  accountId: 'default',
  enabled: true,
  bcsUrl: 'ws://bcs.test/ws/bot',
  botId: 'bot-handler-test',
  botName: 'Handler test bot',
  capabilities: { summary: '', domains: [], skills: [], scopes: [] },
  heartbeatIntervalMs: 60000,
  reconnectIntervalMs: 5000,
  connectionTimeoutMs: 10000,
};

const sessionKey = 'bcs:handler-test';

function request(id: string, runId: string): any {
  return {
    type: 'req',
    id,
    method: 'chat.send',
    params: {
      idempotency_key: runId,
      session_key: sessionKey,
      bcs_session_id: sessionKey,
      bcs_group_id: 'handler-group',
      channel: { source: 'test' },
      session_context: {},
      message: { role: 'user', content: [{ type: 'text', text: 'hello' }], timestamp: Date.now() },
    },
  };
}

function abortRequest(id: string, runId: string): any {
  return {
    type: 'req',
    id,
    method: 'chat.abort',
    params: { session_key: sessionKey, run_id: runId },
  };
}

function setupRuntime(
  dispatch: (args: any) => Promise<void>,
): {
  client: { responses: any[]; events: any[]; sendResponse: Function; sendEvent: Function };
  coordinator: SessionMutationCoordinator;
  getAgentEventHandler: () => ((event: any) => boolean) | undefined;
} {
  let agentEventHandler: ((event: any) => boolean) | undefined;
  const client = {
    responses: [] as any[],
    events: [] as any[],
    sendResponse(id: string, ok: boolean, payload?: unknown, error?: unknown) {
      client.responses.push({ id, ok, payload, error });
    },
    sendEvent(event: string, payload: unknown) {
      client.events.push({ event, payload });
    },
  };
  setBcsRuntime({
    config: { async loadConfig() { return {}; } },
    events: {
      onAgentEvent(handler: (event: any) => boolean) {
        agentEventHandler = handler;
        return () => { agentEventHandler = undefined; };
      },
    },
    channel: {
      routing: {
        resolveAgentRoute() {
          return { agentId: 'agent-handler-test', sessionKey };
        },
      },
      reply: {
        finalizeInboundContext(context: Record<string, unknown>) {
          return context;
        },
        dispatchReplyWithBufferedBlockDispatcher: dispatch,
      },
      session: {
        resolveStorePath() {
          return '/tmp/bcs-handler-session-test.json';
        },
        async recordInboundSession() {
          return undefined;
        },
      },
    },
  } as any);

  const coordinator = new SessionMutationCoordinator();
  initAgentEventsSubscription();
  return {
    client,
    coordinator,
    getAgentEventHandler: () => agentEventHandler,
  };
}

function terminal(
  getAgentEventHandler: () => ((event: any) => boolean) | undefined,
  runId: string,
  phase: 'end' | 'error',
): void {
  getAgentEventHandler()?.({
    runId,
    stream: 'lifecycle',
    ts: Date.now(),
    data: { phase },
  });
}

describe('BCS Session mutation handler integration', () => {
  afterEach(() => {
    cleanupAgentEventsSubscription();
  });

  it('holds inject admission through a normal final lifecycle', async () => {
    let releaseDispatch!: () => void;
    const setup = setupRuntime(async ({ replyOptions }: any) => {
      replyOptions.onAgentRunStart('agent-final');
      await new Promise<void>(resolve => { releaseDispatch = resolve; });
    });
    const send = handleChatSend(request('send-final', 'run-final'), setup.client as any, account, undefined, setup.coordinator);
    await new Promise(resolve => setImmediate(resolve));
    const waiter = setup.coordinator.waitForInject({ botId: account.botId, canonicalSessionKey: sessionKey }, 1000);
    terminal(setup.getAgentEventHandler, 'agent-final', 'end');
    releaseDispatch();
    await send;
    assert.deepEqual(await waiter, { status: 'ready' });
    assert.equal(setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }), false);
  });

  it('releases inject admission after an error lifecycle', async () => {
    let releaseDispatch!: () => void;
    const setup = setupRuntime(async ({ replyOptions }: any) => {
      replyOptions.onAgentRunStart('agent-error');
      await new Promise<void>(resolve => { releaseDispatch = resolve; });
    });
    const send = handleChatSend(request('send-error', 'run-error'), setup.client as any, account, undefined, setup.coordinator);
    await new Promise(resolve => setImmediate(resolve));
    const waiter = setup.coordinator.waitForInject({ botId: account.botId, canonicalSessionKey: sessionKey }, 1000);
    terminal(setup.getAgentEventHandler, 'agent-error', 'error');
    releaseDispatch();
    await send;
    assert.deepEqual(await waiter, { status: 'ready' });
  });

  it('does not release the owner when abort returns before terminal lifecycle', async () => {
    let releaseDispatch!: () => void;
    const setup = setupRuntime(async ({ replyOptions }: any) => {
      replyOptions.onAgentRunStart('agent-abort');
      await new Promise<void>(resolve => { releaseDispatch = resolve; });
    });
    const send = handleChatSend(request('send-abort', 'run-abort'), setup.client as any, account, undefined, setup.coordinator);
    await new Promise(resolve => setImmediate(resolve));

    await handleChatAbort(abortRequest('abort', 'run-abort'), setup.client as any);
    releaseDispatch();
    await send;
    assert.equal(setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }), true);

    terminal(setup.getAgentEventHandler, 'agent-abort', 'error');
    assert.equal(setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }), false);
  });

  it('returns SESSION_BUSY for a second send on the same Session', async () => {
    let releaseDispatch!: () => void;
    const setup = setupRuntime(async ({ replyOptions }: any) => {
      replyOptions.onAgentRunStart('agent-first');
      await new Promise<void>(resolve => { releaseDispatch = resolve; });
    });
    const first = handleChatSend(request('send-first', 'run-first'), setup.client as any, account, undefined, setup.coordinator);
    await new Promise(resolve => setImmediate(resolve));
    const second = handleChatSend(request('send-second', 'run-second'), setup.client as any, account, undefined, setup.coordinator);
    await second;
    assert.equal(setup.client.responses.at(-1)?.error?.code, 'SESSION_BUSY');

    releaseDispatch();
    terminal(setup.getAgentEventHandler, 'agent-first', 'end');
    await first;
  });

  it('retains the owner and run context after abortAllStreams until a late terminal', async () => {
    let releaseDispatch!: () => void;
    const setup = setupRuntime(async ({ replyOptions }: any) => {
      replyOptions.onAgentRunStart('agent-shutdown');
      await new Promise<void>(resolve => { releaseDispatch = resolve; });
    });
    const send = handleChatSend(request('send-shutdown', 'run-shutdown'), setup.client as any, account, undefined, setup.coordinator);
    await new Promise(resolve => setImmediate(resolve));

    abortAllStreams();
    assert.equal(setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }), true);

    releaseDispatch();
    await send;
    assert.equal(setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }), true);

    terminal(setup.getAgentEventHandler, 'agent-shutdown', 'end');
    assert.equal(setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }), false);
  });

  it('does not release the owner when dispatch fails before agent binding', async () => {
    const setup = setupRuntime(async () => {
      throw new Error('dispatch failed before agent binding');
    });
    await handleChatSend(request('send-before-bind-error', 'run-before-bind-error'), setup.client as any, account, undefined, setup.coordinator);

    assert.equal(
      setup.coordinator.hasOwner({ botId: account.botId, canonicalSessionKey: sessionKey }),
      true,
    );
  });
});
