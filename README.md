# UltraStreamBuffer

[![npm version](https://img.shields.io/npm/v/ultra-stream-buffer.svg)](https://www.npmjs.com/package/ultra-stream-buffer)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](./LICENSE)
[![TypeScript](https://img.shields.io/badge/TypeScript-strict-3178C6.svg)](./tsconfig.json)
[![Zero Dependency](https://img.shields.io/badge/deps-0-success.svg)](#features--核心特性)
[![GitHub](https://img.shields.io/badge/GitHub-Sadako--Girlfriend%2Fultra--stream--buffer-181717.svg)](https://github.com/Sadako-Girlfriend/ultra-stream-buffer)

> **EN:** A zero-dependency TypeScript text buffer purpose-built for ultra-high-frequency AI streaming output (**2,000+ characters per second**).
>
> **中文：** 专为超高频（**每秒 2000 字以上**）AI 流式输出设计的**零依赖** TypeScript 文本缓冲器。

### Live Demo · 在线演示

Clay-styled bilingual playground (EN / 中文):

**https://sadako-girlfriend.github.io/ultra-stream-buffer/**

> Enable GitHub Pages once: **Settings → Pages → Source: Deploy from a branch → `main` / `/docs`**.
>
> 首次需在仓库开启 Pages：**Settings → Pages → Source 选分支 `main`，文件夹 `/docs`**。

---

## Why UltraStreamBuffer? · 为什么需要它？

**EN:** LLM / SSE / WebSocket token feeds often arrive faster than the browser can paint. Naively appending to the DOM on every chunk will thrash layout, drop frames, and eventually freeze the main thread.

**中文：** LLM / SSE / WebSocket 的 token 到达速度经常快过浏览器的绘制节奏。如果每个分片都立刻改 DOM，就会疯狂触发布局、掉帧，最终把主线程拖死。

**UltraStreamBuffer** batches those chunks, aligns flushes to `requestAnimationFrame`, and automatically degrades under backlog — so your UI stays responsive while the stream stays complete.

**UltraStreamBuffer** 负责把分片攒批、对齐到 `requestAnimationFrame` 再冲刷，并在积压时自动降级 —— 流完整送达的同时，界面仍然跟得上。

---

## Features · 核心特性

| | EN | 中文 |
| --- | --- | --- |
| 🚀 | **Zero dependency** — pure TypeScript. Works with any frontend framework and Node.js. | **零依赖** — 纯 TypeScript 实现，适配所有前端框架及 Node.js 环境。 |
| 🛡️ | **Frame-aligned flushing** — merges high-frequency pushes via `requestAnimationFrame` to stop UI jank. | **帧率对齐** — 使用 `requestAnimationFrame` 自动合并高频数据，杜绝 UI 掉帧。 |
| 🔪 | **Smart degradation** — when backlog exceeds **2,000 chars** (default) or latency exceeds **100ms** (default), skips the animation frame and force-flushes to protect the main thread. | **智能降级** — 当数据积压超限（默认 **2000** 字）或延迟过高（默认 **100ms**）时，自动跳过动画帧强制冲刷，防止主线程死锁。 |
| 🧹 | **Leak-proof design** — single outstanding frame ticket, idempotent `abort()` / `finish()`, and GC-friendly buffer teardown. | **防泄漏设计** — 单一帧票据、幂等 `abort()` / `finish()`，完美配合浏览器 GC。 |

Node environments without `requestAnimationFrame` automatically fall back to `setTimeout(..., throttleTime)` (default **16ms** ≈ 60 FPS).

无 `requestAnimationFrame` 的 Node 环境会自动回退到 `setTimeout(..., throttleTime)`（默认 **16ms** ≈ 60 FPS）。

---

## Benchmark · 性能实测

> Measured on real hardware under intentional stress. Your mileage may vary with DOM complexity, but the shape of the results is what matters: **the main thread stays alive**.
>
> 以下为真实硬件上的极限压测。不同 DOM 复杂度会有差异，但结论一致：**主线程扛得住**。

**Test machine · 测试设备**

- CPU: **Intel Core i5-6200U** (2015 low-voltage dual-core)
- RAM: **12 GB**
- Runtime: Chromium-based browser, `UltraStreamBuffer` consumer updating a live text panel

### Scenario A · 场景 A — Sustained bombardment · 持续轰炸

| | |
| --- | --- |
| Load | **120 tokens / sec**, continuous stream for **100 seconds** |
| Volume | **~12,000 characters** total |
| 负载 | **120 token/秒**，连续推流 **100 秒** |
| 体量 | 合计约 **1.2 万字** |
| **Result · 结果** | **Zero visible stutter.** Steady **~40 FPS** average on a decade-old ULV CPU. |
| | **肉眼零卡顿**，十年前的低压 U 处理器上平均仍约 **40 FPS**。 |

### Scenario B · 场景 B — Extreme tsunami · 极限海啸

| | |
| --- | --- |
| Load | **1,000 tokens / sec**, single run injecting **100,000 characters** |
| 负载 | **1000 token/秒**，单次推入 **10 万字** |
| **Result · 结果** | **Main thread never locked.** Page never crashed. Skip-frame degradation kicked in and kept rendering stable under backlog. |
| | **主线程未阻塞**，页面未崩溃；积压触发跳帧降级后仍平稳渲染。 |

On paper that is a hostile feed. In practice UltraStreamBuffer coalesces the storm into frame-sized updates — and when even that is not enough, forced flush keeps memory and latency bounded.

纸面上看这是一场恶意喂流；实际上 UltraStreamBuffer 会把风暴收成「按帧更新」，真顶不住时再强制冲刷，把内存与延迟锁在边界内。

---

## Install · 安装

```bash
npm install ultra-stream-buffer
```

```bash
pnpm add ultra-stream-buffer
```

```bash
yarn add ultra-stream-buffer
```

**Runtime dependencies: none.** · **运行时依赖：无。**

---

## Quick Start · 快速开始

```ts
import { UltraStreamBuffer } from 'ultra-stream-buffer';

// Any sink: DOM node, virtual list, terminal, Worker postMessage, …
// 任意出口：DOM、虚拟列表、终端、Worker postMessage…
const sink = document.querySelector('#output')!;

const buffer = new UltraStreamBuffer({
  onUpdate: (text) => {
    // One coalesced string per animation frame (or forced flush).
    // 每个动画帧（或强制冲刷）只会拿到一段已合并的文本。
    sink.textContent += text;
  },
  onFinish: () => {
    console.log('stream complete');
  },
  onError: (err) => {
    console.error(err); // optional · 可选：避免 onUpdate 抛错被静默吞掉
  },

  // Optional knobs · 可选调参
  throttleTime: 16,      // Node / no-RAF fallback interval (ms)
  maxBufferChars: 2000,  // degrade when pending chars ≥ threshold
  maxSpanMs: 100,        // degrade when pending span ≥ threshold (checked on push only)
});

// Feed tokens as fast as they arrive — push() is O(1) amortized.
// 来多快推多快 —— push() 均摊 O(1)。
async function consumeAiStream(stream: AsyncIterable<string>) {
  for await (const chunk of stream) {
    buffer.push(chunk);
  }
  buffer.finish(); // flush remainder + fire onFinish
}

// Cancel instantly if the user navigates away / clicks Stop.
// 用户离开页面或点停止时，瞬间取消。
abortButton.addEventListener('click', () => {
  buffer.abort(); // cancel frame ticket, clear buffer, silence callbacks
});
```

### API surface · API 一览

```ts
interface UltraStreamBufferOptions {
  onUpdate: (text: string) => void;   // required · 必填（运行时也会校验）
  onFinish?: () => void;
  onError?: (error: unknown) => void; // optional error sink · 可选错误出口
  throttleTime?: number;              // default 16, ≥ 0
  maxBufferChars?: number;            // default 2000, ≥ 1
  maxBufferLength?: number;           // alias of maxBufferChars
  maxSpanMs?: number;                 // default 100, ≥ 1; checked on push() only
}

class UltraStreamBuffer {
  push(chunk: string): void;
  finish(): void;
  flushNow(): void;               // sync flush; stream stays open
  abort(): void;
  readonly isAborted: boolean;
  readonly isFinished: boolean;   // true only after finish()
  readonly isDone: boolean;       // finished || aborted
  readonly pendingChars: number;
  readonly hasPendingFrame: boolean;
}
```

> **maxSpanMs note · 说明：** the span check runs **only inside `push()`**. If the upstream stops pushing (e.g. tab hidden → RAF paused + model “thinking”), the library will not spontaneously flush until the next `push`, `finish()`, or degrade path.
>
> **跨度检查只在 `push()` 内执行。** 上游停推时（例如后台标签页 RAF 暂停且模型思考中），在下一次 `push` / `finish` / 降级路径之前不会主动冲刷。

---

## How it works · 工作原理

```
  tokens ──push()──► [ string[], string[], … ] ──RAF / timeout──► onUpdate(merged)
                         │
                         ├─ chars ≥ maxBufferChars ─┐
                         └─ span  ≥ maxSpanMs      ─┴─► cancel frame → sync flush
                            (span checked on push only)
```

1. **`push`** appends into an internal array and schedules **at most one** frame ticket.
2. **`flush`** joins once, clears the array **before** invoking `onUpdate` (so fragmented chunks are not retained), then delivers the merged string. Re-entrant `push` during `onUpdate` is deferred and drained afterward.
3. **`abort`** cancels the ticket, drops pending text, and permanently silences callbacks.
4. **`finish`** force-flushes remainder, then fires `onFinish` once.

1. **`push`** 写入内分数组，且任意时刻最多只挂 **一张** 帧票据。  
2. **`flush`** 只 `join` 一次，**先清空数组再调用** `onUpdate`；`onUpdate` 内重入的 `push` 会延后排空，避免同步递归。  
3. **`abort`** 取消调度、丢弃缓冲，并永久静默回调。  
4. **`finish`** 冲刷残余后，**只触发一次** `onFinish`。

---

## Browser & Node · 环境支持

| Environment | Flush scheduler |
| --- | --- |
| Modern browsers | `requestAnimationFrame` / `cancelAnimationFrame` |
| Node.js ≥ 18 | `setTimeout` fallback (`throttleTime`, default 16ms) |
| Workers | Same detection — RAF if present, otherwise timer |

| 环境 | 刷新调度 |
| --- | --- |
| 现代浏览器 | `requestAnimationFrame` / `cancelAnimationFrame` |
| Node.js ≥ 18 | `setTimeout` 回退（`throttleTime`，默认 16ms） |
| Worker | 同样自动检测 — 有 RAF 用 RAF，否则用定时器 |

---

## License · 开源协议

Released under the [**MIT License**](./LICENSE).

以 [**MIT License**](./LICENSE) 发布 —— 可商用、可修改、可再分发。

---

<p align="center">
  <sub>Built for streams that refuse to slow down. · 为不肯减速的流式输出而生。</sub>
</p>
