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
 *   3. Explicit teardown — abort() clears buffer, cancels ticket, silences callbacks.
 *      显式销毁 — abort() 清空缓冲、取消调度、静默后续回调，杜绝泄漏。
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
   * EN: Fallback frame interval in milliseconds when requestAnimationFrame
   *     is unavailable (e.g. Node.js). Defaults to 16 ≈ 60 fps.
   * 中文：无 requestAnimationFrame 时（如 Node 环境）的回退帧间隔，默认 16ms ≈ 60fps。
   */
  throttleTime?: number;

  /**
   * EN: Character-count threshold that triggers skip-frame degradation.
   *     When the pending buffer exceeds this many characters, the scheduler
   *     immediately flushes instead of waiting for the next animation frame.
   *     Defaults to 2000.
   * 中文：累计字数阈值。当 buffer 内待刷新字符数超过该值时，触发跳帧降级，
   *      立即同步冲刷，不再等待下一帧。默认 2000。
   */
  maxBufferChars?: number;

  /**
   * EN: Alias of maxBufferChars for naming compatibility. Prefer maxBufferChars.
   * 中文：maxBufferChars 的别名，兼容不同命名习惯。新代码请优先使用 maxBufferChars。
   */
  maxBufferLength?: number;

  /**
   * EN: Maximum wall-clock span (ms) from the first pending push to the flush.
   *     If the buffer has been holding data longer than this, force an immediate
   *     flush to prevent unbounded latency. Defaults to 100.
   * 中文：从首次 push 到 flush 的最大允许跨度（毫秒）。超时则强制立即冲刷，
   *      防止延迟无限堆积。默认 100。
   */
  maxSpanMs?: number;
}

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
 * EN: Detect whether the global cancelAnimationFrame API is available.
 * 中文：检测全局是否存在 cancelAnimationFrame。
 */
function hasCancelAnimationFrame(): boolean {
  return typeof cancelAnimationFrame === 'function';
}

/* -------------------------------------------------------------------------- */
/*                              Core Class                                    */
/* -------------------------------------------------------------------------- */

/**
 * UltraStreamBuffer
 *
 * EN: Batches high-frequency string chunks and delivers them on animation-frame
 *     boundaries (or a setTimeout fallback). Includes automatic skip-frame
 *     degradation when volume or latency exceeds configured thresholds.
 *
 * 中文：将高频字符串分片批量合并，在动画帧边界（或 setTimeout 回退）上交付。
 *      当数据量或延迟超过阈值时自动跳帧降级，保证主线程不被阻塞。
 *
 * @example
 * ```ts
 * const buf = new UltraStreamBuffer({
 *   onUpdate: (text) => { target.textContent += text; },
 *   onFinish: () => { console.log('done'); },
 * });
 *
 * // Feed tokens as they arrive / 随到随推
 * buf.push('Hel');
 * buf.push('lo');
 * buf.push(' World');
 *
 * // When the upstream stream ends / 上游流结束时
 * buf.finish();
 *
 * // Or cancel mid-stream / 或中途取消
 * // buf.abort();
 * ```
 */
export class UltraStreamBuffer {
  /* ---- immutable config (set once in constructor) / 构造时固化的配置 ---- */

  /** EN: External update callback. 中文：外部更新回调。 */
  private readonly onUpdate: (text: string) => void;

  /** EN: Optional finish callback. 中文：可选的结束回调。 */
  private readonly onFinish: (() => void) | undefined;

  /** EN: Fallback timer interval (ms). 中文：回退定时器间隔（毫秒）。 */
  private readonly throttleTime: number;

  /** EN: Char-count skip-frame threshold. 中文：字数跳帧阈值。 */
  private readonly maxBufferChars: number;

  /** EN: Max pending span before forced flush (ms). 中文：强制冲刷前的最大等待跨度。 */
  private readonly maxSpanMs: number;

  /* ---- mutable runtime state / 可变运行时状态 ---- */

  /**
   * EN: Internal chunk array. Strings are pushed here and joined on flush.
   *     Cleared after every successful flush to release references.
   * 中文：内部分片数组。push 时写入，flush 时 join，成功后立即清空以释放引用。
   */
  private buffer: string[] = [];

  /**
   * EN: Running character count of all strings currently in `buffer`.
   *     Maintained incrementally so we never re-scan the array on every push.
   * 中文：buffer 中所有字符串的累计字符数。增量维护，避免每次 push 都遍历数组。
   */
  private bufferedChars: number = 0;

  /**
   * EN: Timestamp (Date.now) of the first push after the last flush.
   *     Used to measure pending span for the maxSpanMs degradation check.
   *     0 means "no pending data".
   * 中文：上次 flush 之后第一次 push 的时间戳。用于 maxSpanMs 跨度检测。
   *      0 表示当前没有待刷新数据。
   */
  private firstPendingAt: number = 0;

  /**
   * EN: Handle returned by requestAnimationFrame or setTimeout.
   *     null means no frame is currently scheduled.
   * 中文：requestAnimationFrame 或 setTimeout 返回的句柄。
   *      null 表示当前没有已调度的帧。
   */
  private frameId: number | ReturnType<typeof setTimeout> | null = null;

  /**
   * EN: Whether a flush frame is already scheduled. Prevents duplicate tickets.
   * 中文：是否已有待执行的刷新帧。防止重复调度。
   */
  private scheduled: boolean = false;

  /**
   * EN: Abort latch. Once true, every public method becomes a no-op and
   *     onUpdate / onFinish will never fire again.
   * 中文：中止锁存。一旦为 true，所有公开方法变为空操作，
   *      onUpdate / onFinish 永远不再触发。
   */
  private aborted: boolean = false;

  /**
   * EN: Whether finish() has already been called. Guards against double-fire
   *     of onFinish.
   * 中文：finish() 是否已被调用。防止 onFinish 被重复触发。
   */
  private finished: boolean = false;

  /* ---- constructor / 构造函数 ---- */

  /**
   * EN: Create a new buffer instance.
   * 中文：创建一个新的缓冲器实例。
   *
   * @param options - Configuration / 配置项
   */
  constructor(options: UltraStreamBufferOptions) {
    // Flatten option reads into local fields — no retained options object.
    // 将配置拍平到字段，不保留整个 options 对象引用，减少泄漏面。
    this.onUpdate = options.onUpdate;
    this.onFinish = options.onFinish;
    this.throttleTime = options.throttleTime ?? 16;
    // Prefer maxBufferChars; fall back to maxBufferLength alias, then 2000.
    // 优先 maxBufferChars；其次别名 maxBufferLength；最终默认 2000。
    this.maxBufferChars = options.maxBufferChars ?? options.maxBufferLength ?? 2000;
    this.maxSpanMs = options.maxSpanMs ?? 100;
  }

  /* ---- public API / 公开接口 ---- */

  /**
   * EN: Push a text chunk into the buffer and request a render-aligned flush.
   *     Safe to call at arbitrary frequency — chunks are only concatenated,
   *     never processed synchronously beyond a cheap length check.
   *
   * 中文：将文本分片推入内部数组，并触发渲染对齐的刷新调度。
   *      可任意高频调用 — 分片只做追加，除廉价的长度检查外不做同步重计算。
   *
   * @param chunk - Incoming string fragment / 到来的字符串片段
   */
  push(chunk: string): void {
    // Hard gate: aborted or finished instances silently drop all input.
    // 硬门控：已中止或已结束的实例静默丢弃所有输入。
    if (this.aborted || this.finished) {
      return;
    }

    // Ignore empty chunks — they add noise without content.
    // 忽略空分片 — 无内容却会增加数组噪音。
    if (chunk.length === 0) {
      return;
    }

    // Append to the array buffer. O(1) amortized.
    // 追加到数组缓冲。均摊 O(1)。
    this.buffer.push(chunk);
    this.bufferedChars += chunk.length;

    // Stamp the start of the pending window on the first chunk after a flush.
    // 在 flush 后的第一个分片上记录待刷新窗口的起始时间。
    if (this.firstPendingAt === 0) {
      this.firstPendingAt = Date.now();
    }

    // Skip-frame degradation: if volume or latency exceeds thresholds,
    // flush immediately on the current call stack instead of waiting for RAF.
    // This keeps the main thread from accumulating an unbounded backlog.
    //
    // 跳帧降级：若字数或跨度超阈值，在当前调用栈上立即冲刷，
    // 不再等待 RAF，防止主线程积压无限增长。
    if (this.shouldDegrade()) {
      this.cancelFrame();
      this.flush();
      return;
    }

    // Normal path: schedule at most one pending frame.
    // 正常路径：最多调度一帧。
    this.scheduleFlush();
  }

  /**
   * EN: Signal that the upstream stream has ended. Flushes any remaining
   *     buffered text, then fires onFinish (if provided). After finish(),
   *     further push() calls are ignored.
   *
   * 中文：通知上游流已结束。冲刷残余缓冲文本，然后触发 onFinish（若有）。
   *      finish() 之后的 push() 将被忽略。
   */
  finish(): void {
    if (this.aborted || this.finished) {
      return;
    }

    this.finished = true;
    this.cancelFrame();
    this.flush();

    // Fire onFinish only if we were not aborted mid-flush.
    // 仅在冲刷过程中未被中止时触发 onFinish。
    if (!this.aborted && this.onFinish !== undefined) {
      this.onFinish();
    }
  }

  /**
   * EN: Instantly stop the scheduler, clear the internal buffer, and permanently
   *     silence onUpdate / onFinish. Idempotent — safe to call multiple times.
   *
   * 中文：瞬间停止调度器，清空内部 buffer，永久阻止后续 onUpdate / onFinish。
   *      幂等 — 多次调用安全。
   */
  abort(): void {
    if (this.aborted) {
      return;
    }

    this.aborted = true;
    this.cancelFrame();
    this.clearBuffer();
  }

  /**
   * EN: Returns true if abort() has been called.
   * 中文：若已调用 abort() 则返回 true。
   */
  get isAborted(): boolean {
    return this.aborted;
  }

  /**
   * EN: Returns true if finish() has already been called.
   * 中文：若已调用 finish() 则返回 true。
   */
  get isFinished(): boolean {
    return this.finished;
  }

  /**
   * EN: Current number of characters waiting to be flushed.
   * 中文：当前等待冲刷的字符数。
   */
  get pendingChars(): number {
    return this.bufferedChars;
  }

  /* ---- private helpers / 私有辅助方法 ---- */

  /**
   * EN: Decide whether skip-frame degradation should kick in.
   * 中文：判断是否应触发跳帧降级。
   */
  private shouldDegrade(): boolean {
    // Volume check / 字数检查
    if (this.bufferedChars >= this.maxBufferChars) {
      return true;
    }

    // Span check — only meaningful when we have a pending window.
    // 跨度检查 — 仅在已有待刷新窗口时有意义。
    if (this.firstPendingAt !== 0) {
      const span = Date.now() - this.firstPendingAt;
      if (span >= this.maxSpanMs) {
        return true;
      }
    }

    return false;
  }

  /**
   * EN: Schedule a single flush on the next animation frame (or setTimeout
   *     fallback). No-ops if a frame is already pending — this is the key
   *     coalescing guarantee that keeps push() cheap under load.
   *
   * 中文：在下一动画帧（或 setTimeout 回退）上调度一次 flush。
   *      若已有待执行帧则直接返回 — 这是高负载下 push() 保持廉价的关键合并保证。
   */
  private scheduleFlush(): void {
    if (this.scheduled || this.aborted) {
      return;
    }

    this.scheduled = true;

    if (hasRequestAnimationFrame()) {
      // Browser / DOM environment — align with vsync.
      // 浏览器 / DOM 环境 — 与垂直同步对齐。
      this.frameId = requestAnimationFrame(() => {
        this.frameId = null;
        this.scheduled = false;
        this.flush();
      });
    } else {
      // Node.js / worker without RAF — approximate 60 fps.
      // Node.js / 无 RAF 的 worker — 近似 60fps。
      this.frameId = setTimeout(() => {
        this.frameId = null;
        this.scheduled = false;
        this.flush();
      }, this.throttleTime);
    }
  }

  /**
   * EN: Cancel any outstanding RAF or timer ticket and reset the scheduled flag.
   * 中文：取消任何未完成的 RAF 或定时器票据，并重置 scheduled 标志。
   */
  private cancelFrame(): void {
    if (this.frameId === null) {
      this.scheduled = false;
      return;
    }

    if (
      hasRequestAnimationFrame() &&
      hasCancelAnimationFrame() &&
      typeof this.frameId === 'number'
    ) {
      // RAF handles are numbers in browsers.
      // 浏览器中 RAF 句柄为 number。
      cancelAnimationFrame(this.frameId);
    } else {
      // setTimeout handles — clear regardless of environment.
      // setTimeout 句柄 — 不区分环境一律 clear。
      clearTimeout(this.frameId as ReturnType<typeof setTimeout>);
    }

    this.frameId = null;
    this.scheduled = false;
  }

  /**
   * EN: Merge the buffer into one string, deliver via onUpdate, then clear.
   *     Guarded by the abort latch so a concurrent abort() can never let a
   *     stale callback escape.
   *
   * 中文：将 buffer 合并为长字符串，经 onUpdate 抛出，然后清空。
   *      受 abort 锁存保护，确保并发 abort() 不会让过期回调漏出去。
   */
  private flush(): void {
    // Abort may have raced in between schedule and execution.
    // abort 可能在调度与执行之间抢先发生。
    if (this.aborted) {
      this.clearBuffer();
      return;
    }

    // Nothing to deliver — cheap early exit.
    // 无内容可交付 — 廉价早退。
    if (this.buffer.length === 0) {
      return;
    }

    // Join once. This is the only potentially O(n) string allocation per frame.
    // 只 join 一次。这是每帧唯一可能的 O(n) 字符串分配。
    const text = this.buffer.join('');

    // Release chunk references BEFORE invoking the callback so that if the
    // consumer retains `text`, we still don't keep the fragmented array alive.
    // 在调用回调之前先释放分片引用：即使消费者持有 `text`，
    // 我们也不再保留碎片化数组。
    this.clearBuffer();

    // Deliver. Wrapped in a try/catch so a throwing consumer cannot poison
    // the buffer state machine or leave scheduled flags in a bad state.
    // 交付。用 try/catch 包裹，防止消费者抛错污染缓冲状态机。
    try {
      this.onUpdate(text);
    } catch {
      // Swallow — library must remain stable regardless of consumer errors.
      // 吞掉异常 — 无论消费者如何出错，库自身状态必须保持稳定。
    }
  }

  /**
   * EN: Reset the buffer array and all related counters / timestamps.
   * 中文：重置缓冲数组及所有相关计数器 / 时间戳。
   */
  private clearBuffer(): void {
    // Replace the array rather than splice — lets the old array be GC'd as a
    // whole if nothing else retains it, and avoids keeping a large capacity.
    // 用新数组替换而非 splice — 若无其他引用，旧数组可整体被 GC，
    // 且不会保留过大的底层容量。
    this.buffer = [];
    this.bufferedChars = 0;
    this.firstPendingAt = 0;
  }
}

export default UltraStreamBuffer;
