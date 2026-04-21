# 主力分析项目 · 状态快照

**最后更新**: 2026-04-21 凌晨
**阶段**: 被动观察期（等实测数据积累）

---

## 核心能力一览

| 领域 | 覆盖 | 状态 |
|---|---|---|
| A 股分形因子模型 | 39 因子 IC-adaptive（含 MF-DFA 谱族 + asym）| ✅ 稳定，5 只主力股 |
| 美股技术分析 | 6 只（NVDA/TSLA/GOOGL/AAPL/TCOM/FUTU）| ✅ 稳定 |
| 美股期权策略 | 分形信号映射 7 种情境，v2 已基于回测修正 | ✅ 部署，67.9% 回测胜率（strong_asym_oversold）|
| 跨式策略 | IV Rank + Δα 双指标 | ✅ META 级信号自动识别 |
| 模拟盘交易 | 富途 OpenD + TrdEnv.SIMULATE 硬锁 | ✅ 股票+期权+挂单闭环验证 |
| 实时行情 | 富途 OPRA（美股+期权），yfinance（加密货币/期货）| ✅ |
| 主页公开展示 | GitHub Pages 10 节 | ✅ https://maxwu1978.github.io/stock-analysis/ |
| 真实盘观察 | NVDA 52 股，**仅本地**不公开 | ✅ 隐私保护 |
| 定时自动化 | 3 个 launchd 任务 | ✅ 运行中 |

---

## 文件清单（核心脚本）

### 数据层
- `fetch_data.py` — A 股历史（新浪/腾讯，免费）
- `fetch_us.py` — 美股历史+财报（yfinance）
- `fetch_wencai.py` — 同花顺问财自然语言选股（pywencai）
- `fetch_futu.py` — 富途 OpenD 实时行情+期权链+持仓（只读）
- `fetch_futures_yf.py` — 美股期货 yfinance 日线

### 分析层
- `indicators.py` — 33+ 因子（含 MF-DFA 谱 4 特征）
- `probability.py` / `probability_us.py` — IC-adaptive 加权模型
- `fractal_survey.py` — MF-DFA 核心算法（被多个脚本复用）
- `iv_rank.py` — IV 历史分位（realized vol 代理 + 积累真实 IV 历史）

### 策略层
- `option_fractal_advisor.py` — 分形信号 → 期权策略 v2（单腿 Call/Put）
- `option_straddle_advisor.py` — 跨式策略 + IV Rank
- `option_monitor.py` — 持仓监控 + 智能平仓建议 + 主页片段生成
- `real_position_observer.py` — 真实盘只读观察（本地片段，不公开）
- `option_advisor_backtest.py` — 策略方向性回测
- `signal_hit_rate.py` — 实际触发信号 × yfinance 实际走势 命中率统计

### 执行层（只模拟盘）
- `trade_futu_sim.py` — 硬锁 SIMULATE；支持市价/限价/GTC/STOP/DAY 挂单；`--confirm` 必需

### 自动化
- `run_advisor_daily.sh` + `com.maxwu.fractal-advisor.plist` — 工作日 15:00/22:00 策略推荐（tech_plus 13 股）
- `run_option_monitor.sh` + `com.maxwu.option-monitor.plist` — 每小时持仓监控+主页刷新+自动 push
- `auto_hedge_daily.sh` + `com.maxwu.auto-hedge.plist` — 工作日 15:30 自动挂 DAY 止盈单（幂等）

### 研究（不部署）
- `btc_trend_advisor.py` — BTC 条件策略（**回测失败，37.5% 胜率**，带警告注释）
- `btc_intraday_fractal.py` — BTC 4h 分形回测（**失败，48.8% 胜率**）
- `crypto_fractal_survey.py` — 12 币种分形调研
- `futures_fractal_survey.py` — 美股期货分形调研
- `energy_reverse.py` / `industry_backtest.py` / `industry_fractal.py` 等调研脚本

---

## 主页结构（https://maxwu1978.github.io/stock-analysis/）

| № | Section | 数据源 | 更新频率 |
|---|---|---|---|
| 01 | 最新行情 | A 股 5 只 | GitHub Actions 30min |
| 02 | 趋势概率 | A 股因子模型 | 同上 |
| 03 | 技术指标 | A 股 | 同上 |
| 04 | 最新财报 | A 股 | 同上 |
| 05 | 最新行情（美股） | 6 只含 FUTU | 同上 |
| 06 | 趋势概率（美股） | 美股因子 | 同上 |
| 07 | 技术指标（美股） | 美股 | 同上 |
| 08 | 最新财报（美股） | 美股 | 同上 |
| **09** | **Option Positions 期权持仓** | 模拟盘实时 | **本地 launchd 每小时**，含智能平仓按钮 |

（真实盘 № 10 **不在公开页**，仅本地 `real_position_section.html`）

---

## 定时任务（launchd）

```
com.maxwu.fractal-advisor  (工作日 15:00 + 22:00 CEST)
  → run_advisor_daily.sh → option_fractal_advisor.py tech_plus
  → 13 只美股策略推荐累积到 advisor_history.log (本地)

com.maxwu.option-monitor   (每小时 3600s)
  → run_option_monitor.sh → option_monitor.py + real_position_observer.py
  → 更新主页 № 09 节 → git push (模拟盘数据)
  → 更新本地 real_position_section.html (不推送)
  → macOS 通知如有紧急动作

com.maxwu.auto-hedge       (工作日 15:30 CEST)
  → auto_hedge_daily.sh
  → 幂等扫持仓, 无挂单即挂 DAY +30% 止盈限价
  → 富途服务器盘中自动监控到价成交
```

---

## 模拟盘当前仓位

### 持仓
- **GOOGL 260508 Call $337.5** × 1 张（成本 $12.85，现 $12.23，**-4.82%**）
- **GOOGL 260508 Put $337.5** × 1 张（成本 $10.49，现 $11.34，**+8.10%**）
- NVDA 股票 × 5 股（测试残留）
- 总 PnL 跨式约 $23

### 已挂止盈单（DAY, SUBMITTED）
- Call @$16.71 (order 7783388)
- Put @$13.64 (order 7783390)
- 今晚 22:00 CEST 未成交会被撤销，明日 15:30 auto-hedge 重挂

### 账户资金
- 美股模拟现金 ~$999,000 / 总资产 ~$1,000,000
- 港股模拟 HK$1M（空仓）

---

## 学术级发现

### 1. "分形信号最佳带宽假设" ⭐
跨市场（A 股 50 只 / 美股 31 只 / 美股期货 19 个 / 加密 12 个 / 约 112 样本）验证：

```
asym < 0.15         信号弱 / 噪声多
asym 0.30 - 0.50    最佳带宽 / 高信号质量  ← 铂金, META, ETH
asym > 0.65         触发过频 / 反弹幻觉    ← BTC
```

这是一个原创发现，值得整理成研究笔记。

### 2. 加密货币反转策略**结构性失效**
- BTC 日线 37.5% / ETH 46% / DOT 32% / 4h BTC 48.8% / 整体 <50%
- 24/7 交易 + 叙事驱动 + 大熊市 → 无均值回归

### 3. 美股科技股 BUY_PUT 反转 < 50% 胜率
之前设计的"对称反转"假设（RSI 超买后买 Put）在长期牛市中胜率 48%，已从 v2 逻辑降级为 WAIT。

### 4. BUY_CALL @ strong_asym_oversold 胜率 67.9%
这是**唯一经回测验证的强信号**——美股科技股超卖反弹信号可靠。当前部署使用。

### 5. 行业分形分化
- 白酒 RankIC 符号翻转（趋势股而非反转股）
- 房地产 RankIC -0.10 反转之王
- 半导体 30d 盈亏比 2.09 但 IC 弱（行业 β 非因子预测）

---

## 已知限制

1. **富途模拟盘不支持 GTC / STOP 订单**
   - 止盈只能 DAY 当日单（每日 15:30 自动重挂）
   - 止损无法自动挂，靠 option_monitor 每小时告警 + 手动平仓
   - 真实账户支持，切实盘后可用

2. **美股期货需 CME 订阅 ~$5-10/月**
   - 当前用 yfinance 研究，富途下单路径需订阅才能开通
   - 已决定暂不订阅，期货仅做研究

3. **富途 API 订阅配额**（偶尔撞限）
   - 每次 option_advisor 扫 13 股可能占用配额
   - 一般有余量，稀有失败

4. **加密货币分形策略失败**
   - 反转模型跨币种/跨时间尺度全败
   - 唯一线索：日线跨式 6 样本 83% 胜率（待扩大验证）

---

## 数据留痕

| 文件 | 内容 |
|---|---|
| `trade_sim_log.csv` | 所有模拟交易动作 |
| `advisor_history.log` | 策略推荐历史（launchd 累积）|
| `option_status.log` | 期权持仓快照历史 |
| `option_monitor_cron.log` | launchd 监控日志 |
| `auto_hedge.log` | 自动挂单历史 |
| `iv_history.csv` | IV 数据积累（252 天后可用真实 IV Rank） |
| `optimization_log.md` | 因子优化完整日志 |

---

## 下一步方向

### 近期（等时间流逝）
- [x] ~~GOOGL 跨式测试闭环~~ — **已挂止盈**，等成交/到期
- [ ] 5/8 跨式到期 → 学习 theta/gamma/vega 真实联动
- [ ] 5 月底 → `signal_hit_rate.py` 足够样本评估实际胜率
- [ ] 7 月底 → IV 历史达 252 天，切换为真实 IV Rank

### 已决定搁置
- BTC/ETH/加密货币策略（反转失败，跨式样本不足）
- CME Data Store 订阅（$5-10/月但 ROI 不清）
- IBKR Paper Trading 扩展（GTC/STOP 需求不大）

### 可选后续
- 港股期权（FUTU 港股、腾讯 00700、美团 03690 有 OPRA 权限类似）
- 分形显著股池的非线性条件因子（调研 C 发现方向）
- "最佳带宽假设"深化（整理成独立研究）

---

## 安全承诺

- ✅ `TrdEnv.SIMULATE` 硬锁定在 `trade_futu_sim.py` 顶层常量
- ✅ Claude **永不执行** `buy`/`sell`/`limit_sell`/`stop_sell`/`unlock_trade`
- ✅ 命令打印给用户在终端手动执行
- ✅ 交易密码从不进入代码（OpenD GUI 层处理）
- ✅ 真实盘数据仅本地，不推送 GitHub
- ✅ 单笔美股 $50k / 港股 HK$400k / 期权 $5k 金额上限

---

## 召唤 Claude

下次会话可直接说：
- "看一下当前状态" → 我查持仓/订单/挂单/主页
- "跑 signal_hit_rate" → 我看实际命中率
- "加 XYZ 股到关注池" → 我修改 `WATCHLISTS`
- "系统有问题" → 我诊断 launchd / OpenD
- "做下一步研究" → 回到"可选后续"列表
