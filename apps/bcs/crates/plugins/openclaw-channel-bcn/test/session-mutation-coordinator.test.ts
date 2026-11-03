import assert from 'node:assert/strict';
import test from 'node:test';
import {
  SessionCoordinatorBusyError,
  SessionMutationCoordinator,
} from '../src/session-mutation-coordinator.js';

const key = { botId: 'bot-a', canonicalSessionKey: 'session-1' };

test('reserves the Session owner while the BCS run is pending', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });

  assert.throws(
    () => coordinator.reserve({ ...key, bcsRunId: 'run-b' }),
    (error: unknown) => error instanceof SessionCoordinatorBusyError
      && error.code === 'SESSION_BUSY',
  );
  const pending = coordinator.waitForInject(key, 100);
  coordinator.bindAgentRun({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' });
  coordinator.complete({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' }, true);
  assert.deepEqual(await pending, { status: 'ready' });
});

test('SESSION_BUSY does not release the original owner', () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });

  assert.throws(
    () => coordinator.reserve({ ...key, bcsRunId: 'run-b' }),
    SessionCoordinatorBusyError,
  );
  assert.equal(coordinator.hasOwner(key), true);
  coordinator.complete({ ...key, bcsRunId: 'run-a' }, true);
});

test('different Bots with the same Session key run independently', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  coordinator.reserve({ botId: 'bot-b', canonicalSessionKey: key.canonicalSessionKey, bcsRunId: 'run-b' });
  assert.equal(coordinator.hasOwner({ botId: 'bot-b', canonicalSessionKey: key.canonicalSessionKey }), true);
});

test('different Sessions for one Bot run independently', () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  coordinator.reserve({ botId: key.botId, canonicalSessionKey: 'session-2', bcsRunId: 'run-b' });
  assert.equal(coordinator.hasOwner({ botId: key.botId, canonicalSessionKey: 'session-2' }), true);
});

test('abort keeps inject waiters until terminal cleanup, then releases them', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' });
  const pending = coordinator.waitForInject(key, 1000);

  assert.equal(coordinator.abort({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' }), true);
  assert.equal(coordinator.hasOwner(key), true);
  let released = false;
  void pending.then(() => { released = true; });
  await Promise.resolve();
  assert.equal(released, false);

  coordinator.complete({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' }, true);
  assert.deepEqual(await pending, { status: 'ready' });
  assert.equal(coordinator.hasOwner(key), false);
});

test('multiple inject waiters are released together after terminal cleanup', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  const first = coordinator.waitForInject(key, 1000);
  const second = coordinator.waitForInject(key, 1000);

  coordinator.complete({ ...key, bcsRunId: 'run-a' }, true);
  assert.deepEqual(await Promise.all([first, second]), [
    { status: 'ready' },
    { status: 'ready' },
  ]);
});

test('abort before agent binding still requires matching terminal cleanup', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  const pending = coordinator.waitForInject(key, 1000);

  assert.equal(coordinator.abort({ ...key, bcsRunId: 'run-a' }), true);
  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-a', agentRunId: 'late-agent' }), false);
  assert.equal(coordinator.hasOwner(key), true);
  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-a' }, true), true);
  assert.deepEqual(await pending, { status: 'ready' });
});

test('stale terminal identity cannot release a newer owner', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' });
  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-old', agentRunId: 'agent-a' }), false);
  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-old' }), false);
  assert.equal(coordinator.hasOwner(key), true);
  coordinator.complete({ ...key, bcsRunId: 'run-a', agentRunId: 'agent-a' }, true);
});

test('stale pre-route identity cannot release a rekeyed owner', () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  coordinator.rekey({ ...key, bcsRunId: 'run-a' }, {
    botId: key.botId,
    canonicalSessionKey: 'canonical-session',
  });

  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-a' }), false);
  assert.equal(coordinator.hasOwner({
    botId: key.botId,
    canonicalSessionKey: 'canonical-session',
  }), true);
  coordinator.complete({
    botId: key.botId,
    canonicalSessionKey: 'canonical-session',
    bcsRunId: 'run-a',
  }, true);
});

test('restart releases waiters but retains the owner until the old run terminates', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  const pending = coordinator.waitForInject(key, 1000);
  coordinator.restart();
  assert.deepEqual(await pending, {
    status: 'gateway_restarted',
    reason: 'OpenClaw gateway restarted before the BCS Agent run completed',
  });
  assert.equal(coordinator.hasOwner(key), true);
  assert.deepEqual(await coordinator.waitForInject(key, 1), {
    status: 'gateway_restarted',
    reason: 'OpenClaw gateway disconnected before the BCS Agent run completed',
  });
  coordinator.complete({ ...key, bcsRunId: 'run-a' }, true);
  assert.equal(coordinator.hasOwner(key), false);
  coordinator.reserve({ ...key, bcsRunId: 'run-b' });
  coordinator.complete({ ...key, bcsRunId: 'run-b' }, true);
});

test('failed or timed out waiters do not block later injections', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-a' });
  assert.deepEqual(await coordinator.waitForInject(key, 1), {
    status: 'timed_out',
    reason: 'BCS Agent run did not reach a terminal state',
  });
  coordinator.complete({ ...key, bcsRunId: 'run-a' }, true);
  assert.deepEqual(await coordinator.waitForInject(key, 1), { status: 'ready' });
});

test('timeout preserves the owner until a matching terminal is confirmed', async () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-timeout', agentRunId: 'agent-timeout' });
  const waiter = coordinator.waitForInject(key, 1000);

  assert.equal(coordinator.markTimedOut({
    ...key,
    bcsRunId: 'run-timeout',
    agentRunId: 'agent-timeout',
  }), true);
  assert.deepEqual(await waiter, {
    status: 'timed_out',
    reason: 'BCS Agent run did not reach a terminal state; Session remains locked',
  });
  assert.equal(coordinator.hasOwner(key), true);
  assert.deepEqual(await coordinator.waitForInject(key, 1), {
    status: 'timed_out',
    reason: 'BCS Agent run did not reach a terminal state; Session remains locked',
  });
  assert.equal(coordinator.complete({
    ...key,
    bcsRunId: 'run-timeout',
    agentRunId: 'wrong-agent',
  }), false);
  assert.equal(coordinator.hasOwner(key), true);
  assert.equal(coordinator.complete({
    ...key,
    bcsRunId: 'run-timeout',
    agentRunId: 'agent-timeout',
  }, true), true);
  assert.equal(coordinator.hasOwner(key), false);
});

test('cleanup completion without terminal confirmation cannot release a timed-out owner', () => {
  const coordinator = new SessionMutationCoordinator();
  coordinator.reserve({ ...key, bcsRunId: 'run-timeout' });
  coordinator.markTimedOut({ ...key, bcsRunId: 'run-timeout' });
  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-timeout' }), false);
  assert.equal(coordinator.hasOwner(key), true);
  assert.equal(coordinator.complete({ ...key, bcsRunId: 'run-timeout' }, true), true);
});
