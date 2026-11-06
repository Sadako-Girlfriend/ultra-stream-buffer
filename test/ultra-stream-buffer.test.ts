/**
 * UltraStreamBuffer — basic unit tests (Node.js built-in test runner)
 * 基础单元测试（Node 内置 test runner，零测试框架依赖）
 */
import assert from 'node:assert/strict';
import { describe, it, beforeEach, afterEach } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import {
  UltraStreamBuffer,
  type UltraStreamBufferOptions,
} from '../src/ultra-stream-buffer.ts';

/** Collect onUpdate payloads into an array. */
function createCollector(overrides: Partial<UltraStreamBufferOptions> = {}) {
  const updates: string[] = [];
  let finished = false;

  const buf = new UltraStreamBuffer({
    throttleTime: 10,
    maxBufferChars: 2000,
    maxSpanMs: 1000,
    onUpdate: (text) => {
      updates.push(text);
    },
    onFinish: () => {
      finished = true;
    },
    ...overrides,
  });

  return {
    buf,
    updates,
    get finished() {
      return finished;
    },
  };
}

describe('UltraStreamBuffer', () => {
  // Node has no RAF by default — ensure we exercise the setTimeout path.
  // 默认无 RAF，确保走 setTimeout 回退路径。
  const originalRAF = globalThis.requestAnimationFrame;
  const originalCancelRAF = globalThis.cancelAnimationFrame;

  beforeEach(() => {
    // @ts-expect-error intentional delete for Node fallback coverage
    delete globalThis.requestAnimationFrame;
    // @ts-expect-error intentional delete for Node fallback coverage
    delete globalThis.cancelAnimationFrame;
  });

  afterEach(() => {
    if (originalRAF) {
      globalThis.requestAnimationFrame = originalRAF;
    } else {
      // @ts-expect-error restore absence
      delete globalThis.requestAnimationFrame;
    }
    if (originalCancelRAF) {
      globalThis.cancelAnimationFrame = originalCancelRAF;
    } else {
      // @ts-expect-error restore absence
      delete globalThis.cancelAnimationFrame;
    }
  });

  it('batches multiple push() calls into one onUpdate per frame', async () => {
    const { buf, updates } = createCollector({ throttleTime: 10 });

    buf.push('Hel');
    buf.push('lo');
    buf.push('!');

    assert.equal(updates.length, 0, 'should not flush synchronously');
    assert.equal(buf.pendingChars, 6);

    await delay(30);

    assert.equal(updates.length, 1);
    assert.equal(updates[0], 'Hello!');
    assert.equal(buf.pendingChars, 0);
  });

  it('ignores empty chunks', async () => {
    const { buf, updates } = createCollector({ throttleTime: 10 });

    buf.push('');
    buf.push('a');
    buf.push('');

    await delay(30);

    assert.equal(updates.length, 1);
    assert.equal(updates[0], 'a');
  });

  it('finish() flushes remaining text then fires onFinish', async () => {
    const collector = createCollector({ throttleTime: 50 });

    collector.buf.push('done');
    // finish should flush immediately without waiting for the timer
    collector.buf.finish();

    assert.equal(collector.updates.length, 1);
    assert.equal(collector.updates[0], 'done');
    assert.equal(collector.finished, true);
    assert.equal(collector.buf.isFinished, true);

    // further pushes are ignored
    collector.buf.push('more');
    await delay(60);
    assert.equal(collector.updates.length, 1);
  });

  it('abort() cancels scheduler, clears buffer, and silences callbacks', async () => {
    let updateCount = 0;
    let finishCount = 0;

    const buf = new UltraStreamBuffer({
      throttleTime: 10,
      onUpdate: () => {
        updateCount += 1;
      },
      onFinish: () => {
        finishCount += 1;
      },
    });

    buf.push('will-be-dropped');
    assert.ok(buf.pendingChars > 0);

    buf.abort();

    assert.equal(buf.isAborted, true);
    assert.equal(buf.pendingChars, 0);

    await delay(30);

    assert.equal(updateCount, 0);
    assert.equal(finishCount, 0);

    // subsequent push / finish are no-ops
    buf.push('nope');
    buf.finish();
    assert.equal(updateCount, 0);
    assert.equal(finishCount, 0);
  });

  it('degrades to immediate flush when maxBufferChars is exceeded', () => {
    const updates: string[] = [];
    const buf = new UltraStreamBuffer({
      throttleTime: 1000,
      maxBufferChars: 10,
      maxSpanMs: 10_000,
      onUpdate: (text) => {
        updates.push(text);
      },
    });

    buf.push('12345');
    assert.equal(updates.length, 0, 'under threshold — still scheduled');

    buf.push('67890X'); // total 11 >= 10 → immediate flush
    assert.equal(updates.length, 1);
    assert.equal(updates[0], '1234567890X');
    assert.equal(buf.pendingChars, 0);
  });

  it('degrades to immediate flush when maxSpanMs is exceeded', async () => {
    const updates: string[] = [];
    const buf = new UltraStreamBuffer({
      throttleTime: 1000,
      maxBufferChars: 10_000,
      maxSpanMs: 20,
      onUpdate: (text) => {
        updates.push(text);
      },
    });

    buf.push('a');
    assert.equal(updates.length, 0);

    await delay(30);

    // next push after span exceeded should force sync flush of pending + new
    buf.push('b');
    assert.equal(updates.length, 1);
    assert.equal(updates[0], 'ab');
  });

  it('does not leak a second frame ticket under burst push', async () => {
    const updates: string[] = [];
    const buf = new UltraStreamBuffer({
      throttleTime: 15,
      maxBufferChars: 10_000,
      maxSpanMs: 10_000,
      onUpdate: (text) => {
        updates.push(text);
      },
    });

    for (let i = 0; i < 100; i += 1) {
      buf.push(String(i % 10));
    }

    await delay(40);

    // All 100 digits should coalesce into a single flush (or very few if span
    // somehow forced mid-burst — with maxSpanMs=10000 it must be exactly 1).
    assert.equal(updates.length, 1);
    assert.equal(updates[0]?.length, 100);
  });

  it('survives a throwing onUpdate without poisoning state', async () => {
    let calls = 0;
    const buf = new UltraStreamBuffer({
      throttleTime: 10,
      maxBufferChars: 10_000,
      maxSpanMs: 10_000,
      onUpdate: () => {
        calls += 1;
        throw new Error('consumer boom');
      },
    });

    buf.push('x');
    await delay(30);
    assert.equal(calls, 1);

    // instance remains usable
    buf.push('y');
    await delay(30);
    assert.equal(calls, 2);
    assert.equal(buf.isAborted, false);
  });

  it('abort() is idempotent', () => {
    const buf = new UltraStreamBuffer({
      onUpdate: () => undefined,
    });
    buf.abort();
    buf.abort();
    assert.equal(buf.isAborted, true);
  });

  it('throws if onUpdate is missing', () => {
    assert.throws(
      () =>
        new UltraStreamBuffer({
          // @ts-expect-error intentional invalid options for runtime guard
          onUpdate: undefined,
        }),
      /onUpdate must be a function/
    );
  });

  it('reports onUpdate errors via onError without poisoning state', async () => {
    const errors: unknown[] = [];
    let calls = 0;
    const buf = new UltraStreamBuffer({
      throttleTime: 10,
      maxBufferChars: 10_000,
      maxSpanMs: 10_000,
      onUpdate: () => {
        calls += 1;
        throw new Error('boom');
      },
      onError: (err) => {
        errors.push(err);
      },
    });

    buf.push('x');
    await delay(30);

    assert.equal(calls, 1);
    assert.equal(errors.length, 1);
    assert.ok(errors[0] instanceof Error);

    buf.push('y');
    await delay(30);
    assert.equal(calls, 2);
    assert.equal(errors.length, 2);
    assert.equal(buf.isAborted, false);
  });

  it('isDone is true after finish or abort; isFinished stays false on abort-only', () => {
    const a = new UltraStreamBuffer({ onUpdate: () => undefined });
    a.abort();
    assert.equal(a.isAborted, true);
    assert.equal(a.isFinished, false);
    assert.equal(a.isDone, true);

    const b = new UltraStreamBuffer({ onUpdate: () => undefined });
    b.finish();
    assert.equal(b.isFinished, true);
    assert.equal(b.isDone, true);
  });

  it('defers re-entrant push during onUpdate instead of nested flush', () => {
    const updates: string[] = [];
    let reentered = false;

    const buf = new UltraStreamBuffer({
      throttleTime: 1000,
      maxBufferChars: 5,
      maxSpanMs: 10_000,
      onUpdate: (text) => {
        updates.push(text);
        if (!reentered) {
          reentered = true;
          // Would previously recurse synchronously via degrade → flush.
          // 以前会经降级路径同步递归 flush。
          buf.push('ZZZZZZ');
        }
      },
    });

    buf.push('AAAAAA'); // >= 5 → sync flush
    assert.equal(updates[0], 'AAAAAA');
    // Re-entrant chunk should be delivered (drain), not dropped.
    // 重入分片应被排空交付，不能丢。
    assert.equal(updates.length, 2);
    assert.equal(updates[1], 'ZZZZZZ');
    assert.equal(buf.pendingChars, 0);
  });

  it('hasPendingFrame reflects outstanding timeout ticket', async () => {
    const buf = new UltraStreamBuffer({
      throttleTime: 40,
      maxBufferChars: 10_000,
      maxSpanMs: 10_000,
      onUpdate: () => undefined,
    });

    assert.equal(buf.hasPendingFrame, false);
    buf.push('a');
    assert.equal(buf.hasPendingFrame, true);
    await delay(60);
    assert.equal(buf.hasPendingFrame, false);
  });

  it('flushNow() sync-flushes without ending the stream', async () => {
    const updates: string[] = [];
    let finished = false;
    const buf = new UltraStreamBuffer({
      throttleTime: 1000,
      maxBufferChars: 10_000,
      maxSpanMs: 10_000,
      onUpdate: (text) => {
        updates.push(text);
      },
      onFinish: () => {
        finished = true;
      },
    });

    buf.push('Hi');
    assert.equal(buf.hasPendingFrame, true);

    buf.flushNow();

    assert.equal(updates.length, 1);
    assert.equal(updates[0], 'Hi');
    assert.equal(buf.pendingChars, 0);
    assert.equal(buf.hasPendingFrame, false);
    assert.equal(buf.isFinished, false);
    assert.equal(buf.isDone, false);
    assert.equal(finished, false);

    // stream still open
    buf.push('!');
    buf.flushNow();
    assert.equal(updates.length, 2);
    assert.equal(updates[1], '!');

    // after finish, flushNow is a no-op
    buf.finish();
    assert.equal(finished, true);
    buf.push('nope');
    buf.flushNow();
    assert.equal(updates.length, 2);
  });
});
