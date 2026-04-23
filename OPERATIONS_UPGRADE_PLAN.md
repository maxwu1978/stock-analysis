# 操盘工程升级实施清单

最后更新：2026-04-23

## 目标

把当前“研究与展示系统”升级为“更接近操盘系统”的工程闭环，优先补齐：

1. 仓位规则引擎
2. 统一退出规则
3. 生产复盘与命中率闭环

本轮不做：

- 重写主模型
- 新增大规模股票池
- 自动实盘下单

## 当前问题

### 1. 信号与仓位脱节

当前 A 股 / 美股模型与期权顾问能输出方向、可靠度、宏观覆盖和概率，但没有统一给出：

- 是否允许开仓
- 建议仓位
- 单笔最大亏损预算
- 同类信号的风险折扣

### 2. 退出逻辑分散

当前退出规则分散在：

- `option_monitor.py` 的观察/平仓建议
- `trade_futu_sim.py` 的限价卖/STOP 命令
- 顾问脚本的自然语言建议

问题是规则口径没有统一模块，不便于复用和复盘。

### 3. 生产复盘只到“方向是否正确”

当前 `signal_hit_rate.py` 主要回答：

- 信号方向对不对

但操盘更需要：

- 建议仓位是否合理
- 实际执行是否偏离计划
- 退出是否按规则发生
- 哪类信号在生产上应该停用

## 设计原则

1. 不破坏现有自动化链路
2. 先做“建议层”和“复盘层”，不做自动执行
3. 统一输出结构，便于页面/日志/后续脚本复用
4. 让可靠度和宏观覆盖直接进入交易决策，而不是只停留在展示层

## 第一阶段交付物

### A. 仓位规则引擎

新增模块：`position_sizing.py`

输入：

- `score`
- `direction`
- `reliability`
- `confidence`
- `macro_penalty`
- `account_equity`
- `entry_price`
- `instrument_type`

输出：

- `allowed`
- `qty`
- `risk_budget`
- `capital_at_risk`
- `position_tier`
- `sizing_note`

验收标准：

- 弱信号默认不建议主动开仓
- 宏观惩罚会缩小仓位
- 单腿期权、跨式、股票使用不同预算口径

### B. 统一退出规则

新增模块：`exit_rules.py`

输入：

- 开仓价格
- 天数
- 策略类型
- 置信度 / 可靠度

输出：

- `take_profit`
- `take_profit_partial`
- `soft_stop`
- `hard_stop`
- `review_days`
- `time_stop_days`
- `notes`

验收标准：

- 单腿期权与跨式使用不同退出模板
- 输出能直接被顾问脚本展示
- 后续能被监控脚本或执行脚本消费

### C. 生产复盘骨架

新增模块：`production_review.py`

输入：

- `advisor_history.log`
- `trade_sim_log.csv`
- `option_status.log`

输出：

- 终端摘要
- `production_signal_scorecard.csv`
- `production_execution_scorecard.csv`

验收标准：

- 能统计真实可解析信号数量
- 能统计模拟交易执行动作数量
- 能识别“有信号无执行”和“有执行无计划”的缺口

## 第二阶段接入点

### 顾问层

- `option_fractal_advisor.py`
- `option_straddle_advisor.py`

接入后新增输出：

- 仓位建议
- 风险预算
- 退出计划

### 执行层

- `trade_futu_sim.py`

第二阶段建议：

- 让执行命令可选择携带“计划注释”
- 让交易日志更容易回放到策略计划

### 复盘层

- `signal_hit_rate.py`
- `production_review.py`

第二阶段建议：

- 区分研究命中率与生产执行质量
- 增加按策略 / 按标的 / 按动作的固定报表

## 阶段计划

### Phase 1

- 文档落盘
- 仓位、退出、复盘三模块建好
- 期权顾问脚本接入

### Phase 2

- 执行日志结构补强
- 复盘报告细化
- 页面或本地报告增加“交易计划/执行评分卡”

### Phase 3

- 仓位规则扩到 A 股 / 美股主模型
- 让可靠度标签真正影响候选池和动作层

## 当前验收清单

- [ ] `OPERATIONS_UPGRADE_PLAN.md`
- [ ] `position_sizing.py`
- [ ] `exit_rules.py`
- [ ] `production_review.py`
- [ ] `option_fractal_advisor.py` 接入仓位与退出建议
- [ ] `option_straddle_advisor.py` 接入仓位与退出建议
- [ ] 编译验证通过
- [ ] 复盘脚本可运行
