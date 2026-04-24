# 主力分析项目 · iOS App 改造方案

**更新时间**: 2026-04-23  
**结论**: 可改造成 iOS App，但不建议把现有 Python 分析引擎直接塞进 iOS 端。本项目最合理的路径是：

`Python 分析引擎`
→ `标准化 JSON 快照`
→ `SwiftUI iOS App`

---

## 1. 当前项目不适合直接“搬进 iOS”的原因

当前仓库核心是 Python 脚本体系：

- 数据抓取：`fetch_data.py` / `fetch_us.py` / `fetch_futu.py`
- 模型分析：`probability.py` / `probability_us.py`
- 策略顾问：`option_fractal_advisor.py` / `option_straddle_advisor.py`
- 页面输出：`generate_page.py` → `docs/*.html`

这些脚本依赖：

- `requests / pandas / numpy`
- 富途 OpenD 本地连接
- 本地文件读写
- launchd 定时任务

其中最关键的一点是：

**iOS App 不能像 macOS 终端那样，直接自由运行这整套本地 Python + OpenD + 文件系统流程。**

所以如果硬做“把 Python 原样塞进 App”，维护成本会很高，稳定性也差。

---

## 2. 推荐架构

推荐改造成三层结构：

### A. 分析引擎层

保留当前 Python 系统不动，继续负责：

- 数据抓取
- 模型打分
- 策略判断
- 期权池扫描
- 页面与报告生成

### B. 数据接口层

新增一层“面向移动端”的标准化输出，不再只吐 HTML：

- `mobile_overview.json`
- `mobile_cn.json`
- `mobile_us.json`
- `mobile_strategy.json`
- `mobile_options.json`
- `mobile_review.json`

这些文件可以由 `generate_page.py` 或新脚本统一生成。

### C. iOS 前端层

用 `SwiftUI` 做原生 App，职责是：

- 读取远端 JSON / 本地静态 JSON
- 展示总览、A 股、美股、策略、期权、复盘
- 做交互、筛选、刷新、跳转

---

## 3. 最现实的落地方案

### 方案 A：原生 SwiftUI + JSON

这是最推荐的方案。

优点：

- 结构清晰
- 易维护
- 页面更适合移动端
- 后续能加通知、收藏、提醒、深链

缺点：

- 需要补一层 JSON 输出
- 要重做前端展示

### 方案 B：SwiftUI + WKWebView 壳

即 iOS App 内直接打开当前 GitHub Pages 或本地 HTML。

优点：

- 改造最快
- 几乎不动分析逻辑

缺点：

- 只是“App 壳”，不是原生体验
- 交互受限
- 后续要做提醒/状态管理/本地缓存比较别扭

### 方案 C：iOS 本地跑 Python

不建议。

原因：

- 集成复杂
- 富途 OpenD 无法在 iPhone 本地正常运行
- 后续维护成本高

---

## 4. 推荐执行路径

### Phase 0：环境前提

当前这台机器**不能直接编 iOS App**，因为没有完整 Xcode。

当前检查结果：

- `swift --version` 可用
- `xcodebuild` 不可用
- 系统提示当前 active developer dir 是：
  `/Library/Developer/CommandLineTools`

要想在终端里跑 iOS 编译，需要先安装完整 Xcode，并切换：

```bash
sudo xcode-select -s /Applications/Xcode.app/Contents/Developer
```

然后再验证：

```bash
xcodebuild -version
```

### Phase 1：先补移动端 JSON 契约

这是第一优先级。

建议新增：

- `mobile_export.py`
  或
- 直接在 `generate_page.py` 里追加 JSON 导出

首批输出字段建议：

#### `mobile_overview.json`

- 更新时间
- 执行摘要
- A 股可靠度摘要
- 美股可靠度摘要
- 期权摘要
- 今日可执行/观察信号数

#### `mobile_strategy.json`

- 明确信号列表
- 观察名单
- 期权机会全池扫描结果
- 每条策略的：
  - symbol
  - action
  - strength
  - reliability
  - position_tier
  - qty
  - risk_budget
  - note

#### `mobile_options.json`

- 当前持仓
- PnL
- 退出模板
- 触发状态
- 期权机会扫描结果

### Phase 2：建 iOS 工程骨架

建议目录：

```text
iOSApp/
  MainDesk.xcodeproj
  MainDesk/
    App/
    Features/
      Overview/
      China/
      US/
      Strategy/
      Options/
      Review/
    Core/
      Networking/
      Models/
      DesignSystem/
```

建议 Tab 结构：

- Overview
- CN
- US
- Strategy
- Options
- Review

### Phase 3：先做“半原生”

第一版不需要一次重做所有页面。

建议：

- Overview / Strategy / Options 用原生 SwiftUI
- 深度详情先用 `WKWebView` 或精简表格承接

这样可以更快上线第一版。

### Phase 4：逐步原生化

后续再把：

- 趋势表
- 技术指标
- 财报表
- 复盘页

逐步从 HTML 渲染切到原生 SwiftUI。

---

## 5. 终端运行的定义

如果你的要求是：

**“iOS App 项目能在终端里编译/跑模拟器”**

那目标命令应该是：

```bash
xcodebuild -project iOSApp/MainDesk.xcodeproj \
  -scheme MainDesk \
  -destination 'platform=iOS Simulator,name=iPhone 16' \
  build
```

或者：

```bash
xcodebuild -project iOSApp/MainDesk.xcodeproj \
  -scheme MainDesk \
  -destination 'platform=iOS Simulator,name=iPhone 16' \
  run
```

但这一步必须以**完整 Xcode 已安装**为前提。

---

## 6. 我对当前项目的改造建议

最推荐的路线是：

1. 不动现有 Python 分析内核
2. 新增移动端 JSON 输出
3. 用 SwiftUI 做原生壳
4. 第一版先做总览 / 策略 / 期权
5. 后面再把 A 股 / 美股详情原生化

一句话说：

**把当前项目改造成 iOS App 是可行的，但应该把“分析引擎”和“移动端展示”拆开，而不是把 Python 原样搬进手机。**

---

## 7. 下一步建议

下一步最值得做的是下面两项中的第 1 项：

1. 先补 `mobile_*.json` 输出层
2. 再建一个最小 SwiftUI 工程骨架

只要第 1 步做好，后面 iOS 前端就会简单很多。
