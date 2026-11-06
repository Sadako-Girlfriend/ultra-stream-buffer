/**
 * =============================================================================
 * UltraStreamBuffer
 * -----------------------------------------------------------------------------
 * EN: A zero-dependency, high-frequency text stream buffer that aligns flushes
 *     with the display refresh cycle (requestAnimationFrame). Designed for
 *     streaming LLM / SSE / WebSocket token feeds without blocking the main thread.
 *
 * 中文：零依赖的高频文本流缓冲器。将多次 push 合并后，对齐到渲染帧
 *      （requestAnimationFrame）再一次性抛出，专为 LLM / SSE / WebSocket
 *      流式输出设计，避免主线程被高频回调阻塞。
 *
 * Design principles / 设计原则:
 *   1. Flat control flow — no nested schedulers, no promise chains.
 *      扁平控制流 — 无嵌套调度器、无 Promise 链。
 *   2. Single outstanding frame ticket — at most one RAF / timer pending.
 *      单一帧票据 — 任意时刻最多只有一个待执行的 RAF / 定时器。
 *      Invariant: a ticket exists iff `frameId !== null` (and `schedulerKind !== null`).
 *      不变量：有票据 ⇔ `frameId !== null`（同时 `schedulerKind !== null`）。
 *   3. Explicit teardown — abort() clears buffer, cancels ticket, silences callbacks.
 *      显式销毁 — abort() 清空缓冲、取消调度、静默后续回调，杜绝泄漏。
 *
 * Notes / 说明:
 *   - maxSpanMs is evaluated only inside push(). It is NOT a hard wall-clock
 *     deadline if pushes stop (e.g. page hidden + RAF paused + upstream idle).
 *   - maxSpanMs 只在 push() 内检查。若上游停推（例如页面隐藏导致 RAF 暂停且
 *     LLM 暂无输出），它不会主动触发冲刷——不是硬实时上限。
 * =============================================================================
 */

/* -------------------------------------------------------------------------- */
/*                              Public Interfaces                             */
/* -------------------------------------------------------------------------- */

/**
 * EN: Configuration object passed to the UltraStreamBuffer constructor.
 * 中文：构造函数配置项。
 */
export interface UltraStreamBufferOptions {
  /**
   * EN: Called once per flush cycle with the concatenated text from the buffer.
   * 中文：每个刷新周期调用一次，参数为 buffer 内所有字符串合并后的长文本。
   */
  onUpdate: (text: string) => void;

  /**
   * EN: Optional. Invoked after finish() has flushed any remaining data.
   * 中文：可选。在 finish() 冲刷完残余数据后触发。
   */
  onFinish?: () => void;

  /**
   * EN: Optional. Invoked when onUpdate (or onFinish) throws.
   *     The buffer state machine stays intact either way.
   * 中文：可选。当 onUpdate（或 onFinish）抛错时触发。
   *      无论是否提供，缓冲状态机都不会被打乱。
   */
  onError?: (error: unknown) => void;

  /**
   * EN: Fallback frame interval in milliseconds when requestAnimationFrame
   *     is unavailable (e.g. Node.js). Defaults to 16 ≈ 60 fps. Clamped to ≥ 0.
   * 中文：无 requestAnimationFrame 时（如 Node 环境）的回退帧间隔，默认 16ms ≈ 60fps。
   *      会被钳制为 ≥ 0。
   */
  throttleTime?: number;

  /**
   * EN: Character-count threshold that triggers skip-frame degradation.
   *     When pending chars reach or exceed this value, flush synchronously
   *     instead of waiting for the next animation frame. Defaults to 2000.
   *     Clamped to ≥ 1.
   * 中文：累计字数阈值。当 buffer 内待刷新字符数达到或超过该值时，触发跳帧降级，
   *      立即同步冲刷，不再等待下一帧。默认 2000。会被钳制为 ≥ 1。
   */
  maxBufferChars?: number;

  /**
   * EN: Alias of maxBufferChars for naming compatibility. Prefer maxBufferChars.
   * 中文：maxBufferChars 的别名，兼容不同命名习惯。新代码请优先使用 maxBufferChars。
   */
  maxBufferLength?: number;

  /**
   * EN: Maximum pending span (ms) checked on each push(), measured from the
   *     first pending chunk after the last flush. When the span reaches or
   *     exceeds this value, force an immediate flush. Defaults to 100.
   *     Clamped to ≥ 1.
   *
   *     Important: this check runs only inside push(). If pushes stop, the
   *     span is not re-evaluated until the next push (RAF may also pause while
   *     the page is hidden).
   *
   * 中文：待刷新跨度上限（毫秒），从上次 flush 后首次 push 起算；在每次 push()
   *      时检查。达到或超过则强制立即冲刷。默认 100。会被钳制为 ≥ 1。
   *
   *      重要：只在 push() 内检查。若上游停推，在下一次 push 之前不会再评估
   *      （页面隐藏时 RAF 也可能完全暂停）。
   */
  maxSpanMs?: number;
}

/** EN: Which timer API owns the outstanding frame ticket. 中文：当前帧票据所属的调度 API。 */
type SchedulerKind = 'raf' | 'timeout';

/* -------------------------------------------------------------------------- */
/*                              Environment Helpers                           */
/* -------------------------------------------------------------------------- */

/**
 * EN: Detect whether the global requestAnimationFrame API is available.
 * 中文：检测全局是否存在 requestAnimationFrame。
 */
function hasRequestAnimationFrame(): boolean {
  return typeof requestAnimationFrame === 'function';
}

/**
 * EN: Monotonic clock when available; falls back to Date.now().
 * 中文：优先单调时钟；不可用时回退 Date.now()。
 */
function nowMs(): number {
  if (typeof performance !== 'undefined' && typeof performance.now === 'function') {
    return performance.now();
  }
  return Date.now();
}

/**
 * EN: Clamp a finite number to [min, +∞). Non-finite → fallback.
 * 中文：将有限数字钳到 [min, +∞)；非有限则用 fallback。
 */
function clampMin(value: number | undefined, min: number, fallback: number): number {
  const n = value ?? fallback;
  if (!Number.isFinite(n)) {
    return fallback;
  }
  return n < min ? min : n;
}

/* -------------------------------------------------------------------------- */
/*                              Core Class                                    */
/* -------------------------------------------------------------------------- */

/**
 * UltraStreamBuffer
 *
 * EN: Batches high-frequency string chunks and delivers them on animation-frame
 *     boundaries (or a setTimeout fallback). Includes automatic skip-frame
 *     degradation when volume or latency thresholds are reached.
 *
 * 中文：将高频字符串分片批量合并，在动画帧边界（或 setTimeout 回退）上交付。
 *      当数据量或延迟达到阈值时自动跳帧降级，保证主线程不被阻塞。
 *
 * @example
 * ```ts
 * const buf = new UltraStreamBuffer({
 *   onUpdate: (text) => { target.textContent += text; },
 *   onFinish: () => { console.log('done'); },
 *   onError: (err) => { console.error(err); },
 * });
 *
 * buf.push('Hel');
 * buf.push('lo');
 * buf.finish();
 * // buf.abort();
 * ```
 */
export class UltraStreamBuffer {
  /* ---- immutable config / 构造时固化的配置 ---- */

  private readonly onUpdate: (text: string) => void;
  private readonly onFinish: (() => void) | undefined;
  private readonly onError: ((error: unknown) => void) | undefined;
  private readonly throttleTime: number;
  private readonly maxBufferChars: number;
  private readonly maxSpanMs: number;

  /* ---- mutable runtime state / 可变运行时状态 ---- */

  /**
   * EN: Internal chunk array. Replaced (not spliced) on clear for GC friendliness.
   * 中文：内部分片数组。清空时整体替换而非 splice，利于 GC。
   */
  private buffer: string[] = [];

  /**
   * EN: Incremental char count of `buffer`. Avoids re-scanning on every push.
   * 中文：buffer 累计字符数。增量维护，避免每次 push 遍历。
   */
  private bufferedChars: number = 0;

  /**
   * EN: performance.now() (or Date.now) of the first push after last flush.
   *     null means "no pending data".
   * 中文：上次 flush 后首次 push 的时间戳。null 表示无待刷新数据。
   */
  private firstPendingAt: number | null = null;

  /**
   * EN: Outstanding frame handle. null ⇔ no ticket scheduled.
   * 中文：未完成的帧句柄。null ⇔ 当前无已调度票据。
   */
  private frameId: number | ReturnType<typeof setTimeout> | null = null;

  /**
   * EN: Which API created `frameId`. Set together with frameId; cleared on cancel.
   * 中文：创建 frameId 时使用的 API。与 frameId 一同设置/清空，取消时按此分支。
   */
  private schedulerKind: SchedulerKind | null = null;

  /**
   * EN: Re-entrancy guard for flush(). Prevents nested onUpdate → push → flush stacks.
   * 中文：flush() 重入锁。防止 onUpdate → push → flush 形成同步递归。
   */
  private flushing: boolean = false;

  /**
   * EN: If re-entrant pushes left data while flushing, drain after the outer flush.
   * 中文：若 flush 期间重入 push 留下了数据，外层 flush 结束后再排空。
   */
  private needsDrain: boolean = false;

  private aborted: boolean = false;
  private finished: boolean = false;

  /**
   * EN: Create a new buffer instance.
   * 中文：创建一个新的缓冲器实例。
   */
  constructor(options: UltraStreamBufferOptions) {
    if (typeof options?.onUpdate !== 'function') {
      throw new TypeError('UltraStreamBuffer: options.onUpdate must be a function');
    }

    this.onUpdate = options.onUpdate;
    this.onFinish = options.onFinish;
    this.onError = options.onError;
    this.throttleTime = clampMin(options.throttleTime, 0, 16);
    this.maxBufferChars = clampMin(
      options.maxBufferChars ?? options.maxBufferLength,
      1,
      2000
    );
    this.maxSpanMs = clampMin(options.maxSpanMs, 1, 100);
  }

  /* ---- public API / 公开接口 ---- */

  /**
   * EN: Push a text chunk and request a render-aligned flush.
   * 中文：推入文本分片，并请求渲染对齐的刷新。
   */
  push(chunk: string): void {
    if (this.aborted || this.finished) {
      return;
    }

    if (chunk.length === 0) {
      return;
    }

    this.buffer.push(chunk);
    this.bufferedChars += chunk.length;

    if (this.firstPendingAt === null) {
      this.firstPendingAt = nowMs();
    }

    // Re-entrant push during onUpdate: keep data, mark drain, avoid nested flush.
    // 在 onUpdate 内重入 push：保留数据、标记待排空，避免嵌套 flush。
    if (this.flushing) {
      this.needsDrain = true;
      return;
    }

    if (this.shouldDegrade()) {
      this.cancelFrame();
      this.flush();
      return;
    }

    this.scheduleFlush();
  }

  /**
   * EN: End the stream: flush remainder, then fire onFinish once.
   * 中文：结束流：冲刷残余，然后触发一次 onFinish。
   */
  finish(): void {
    if (this.aborted || this.finished) {
      return;
    }

    this.finished = true;
    this.cancelFrame();
    this.flush();

    if (!this.aborted && this.onFinish !== undefined) {
      try {
        this.onFinish();
      } catch (error) {
        this.reportError(error);
      }
    }
  }

  /**
   * EN: Cancel scheduler, clear buffer, silence callbacks. Idempotent.
   * 中文：取消调度、清空缓冲、静默回调。幂等。
   */
  abort(): void {
    if (this.aborted) {
      return;
    }

    this.aborted = true;
    this.cancelFrame();
    this.clearBuffer();
    this.needsDrain = false;
  }

  /** EN: Whether abort() was called. 中文：是否已 abort。 */
  get isAborted(): boolean {
    return this.aborted;
  }

  /**
   * EN: Whether finish() was called. Unaffected by abort-only teardown
   *     (abort without finish ⇒ isFinished stays false). Prefer isDone for
   *     “no more work will happen”.
   * 中文：是否已调用 finish()。仅 abort 时不会变为 true。
   *      若只需判断“不会再有后续工作”，请用 isDone。
   */
  get isFinished(): boolean {
    return this.finished;
  }

  /**
   * EN: True when the instance is terminal — finished or aborted.
   * 中文：实例已终止（finish 或 abort）时为 true。
   */
  get isDone(): boolean {
    return this.finished || this.aborted;
  }

  /** EN: Characters waiting to flush. 中文：等待冲刷的字符数。 */
  get pendingChars(): number {
    return this.bufferedChars;
  }

  /**
   * EN: Whether a frame ticket is currently outstanding.
   * 中文：当前是否有未执行的帧票据。
   */
  get hasPendingFrame(): boolean {
    return this.frameId !== null;
  }

  /* ---- private helpers / 私有辅助方法 ---- */

  private shouldDegrade(): boolean {
    if (this.bufferedChars >= this.maxBufferChars) {
      return true;
    }

    if (this.firstPendingAt !== null) {
      const span = nowMs() - this.firstPendingAt;
      if (span >= this.maxSpanMs) {
        return true;
      }
    }

    return false;
  }

  /**
   * EN: Schedule at most one flush ticket. Kind is recorded at schedule time.
   * 中文：最多调度一张刷新票据；调度种类在调度时记录。
   */
  private scheduleFlush(): void {
    if (this.frameId !== null || this.aborted) {
      return;
    }

    if (hasRequestAnimationFrame()) {
      this.schedulerKind = 'raf';
      this.frameId = requestAnimationFrame(() => {
        this.frameId = null;
        this.schedulerKind = null;
        this.flush();
      });
    } else {
      this.schedulerKind = 'timeout';
      this.frameId = setTimeout(() => {
        this.frameId = null;
        this.schedulerKind = null;
        this.flush();
      }, this.throttleTime);
    }
  }

  /**
   * EN: Cancel using the kind recorded at schedule time — never re-detect env.
   * 中文：按调度时记录的种类取消——绝不在取消时重新探测环境。
   */
  private cancelFrame(): void {
    if (this.frameId === null) {
      this.schedulerKind = null;
      return;
    }

    if (this.schedulerKind === 'raf') {
      cancelAnimationFrame(this.frameId as number);
    } else if (this.schedulerKind === 'timeout') {
      clearTimeout(this.frameId as ReturnType<typeof setTimeout>);
    }

    this.frameId = null;
    this.schedulerKind = null;
  }

  /**
   * EN: Join → clear → onUpdate. Re-entrant pushes are deferred via needsDrain.
   * 中文：join → 清空 → onUpdate。重入 push 通过 needsDrain 延后排空。
   */
  private flush(): void {
    if (this.aborted) {
      this.clearBuffer();
      this.needsDrain = false;
      return;
    }

    if (this.buffer.length === 0) {
      return;
    }

    if (this.flushing) {
      this.needsDrain = true;
      return;
    }

    this.flushing = true;
    try {
      const text = this.buffer.join('');
      this.clearBuffer();

      try {
        this.onUpdate(text);
      } catch (error) {
        this.reportError(error);
      }
    } finally {
      this.flushing = false;
    }

    // Drain data pushed re-entrantly during onUpdate.
    // 排空 onUpdate 期间重入 push 留下的数据。
    if (this.needsDrain) {
      this.needsDrain = false;
      this.drainAfterReentrantPush();
    }
  }

  /**
   * EN: After a re-entrant push, either force-flush or schedule — never drop.
   * 中文：重入 push 之后，强制冲刷或重新调度——绝不丢数据。
   */
  private drainAfterReentrantPush(): void {
    if (this.aborted || this.buffer.length === 0) {
      return;
    }

    if (this.finished || this.shouldDegrade()) {
      this.cancelFrame();
      this.flush();
      return;
    }

    this.scheduleFlush();
  }

  private reportError(error: unknown): void {
    if (this.onError === undefined) {
      return;
    }
    try {
      this.onError(error);
    } catch {
      // Secondary errors from onError must not poison the state machine.
      // onError 自身抛错也不能污染状态机。
    }
  }

  private clearBuffer(): void {
    this.buffer = [];
    this.bufferedChars = 0;
    this.firstPendingAt = null;
  }
}

export default UltraStreamBuffer;
