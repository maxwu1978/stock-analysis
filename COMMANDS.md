# 命令索引

统一入口优先使用 `./manage.py <command>`。如果需要查看实时命令表：

```bash
./manage.py list-commands
```

## 页面与发布

| 命令 | 用途 |
|---|---|
| `./manage.py refresh-page` | 重新生成 `docs/` 页面，默认带 `--allow-partial`。 |
| `./manage.py smoke-test` | 本地页面关键标记检查。 |
| `./manage.py smoke-test --remote` | 检查线上 GitHub Pages 关键标记。 |
| `./manage.py doctor` | 检查本地自动化入口、shell 可执行位、plist 语法和旧路径硬编码。 |
| `./manage.py preflight` | 发布前固定检查：doctor、Python 编译、本地页面 smoke、diff 空白检查。 |
| `./manage.py preflight --remote` | 发布前检查并追加线上 smoke。 |

## A 股与行业

| 命令 | 用途 |
|---|---|
| `./manage.py scan-cn` | 运行 A 股机会扫描。 |
| `./manage.py validate-a --years 2` | 用近 2 年数据验证当前 A 股信号规则。 |
| `./manage.py capital-flow-backtest` | 回测主力资金意图标签。 |
| `./manage.py industry-heat` | 生成行业热度和潜力分析。 |
| `./manage.py industry-heat --focus us-ai` | 聚焦美股 AI/制造链。 |

## 美股期权

| 命令 | 用途 |
|---|---|
| `./manage.py refresh-options` | 本地生成当前期权策略输出。 |
| `./manage.py option-signal-review --period 2y` | 用历史数据回放强/弱期权信号。 |
| `./manage.py option-pnl-review --execution-filter` | 用当前执行过滤器复测历史期权代理 PnL。 |
| `./manage.py option-account-sim --initial-capital 10000` | 用历史交易结果模拟账户资金占用和释放。 |
| `./manage.py import-option-chains --input vendor.csv` | 导入第三方历史期权链 CSV。 |
| `./manage.py fetch-option-chains --max-targets 10` | 从免费源抓取策略所需历史期权链切片。 |

## 因子学习

| 命令 | 用途 |
|---|---|
| `./manage.py factor-learn --duration-min 60` | 持续生成、去重、筛选候选因子。 |
| `./manage.py factor-test` | 复测候选因子并更新晋升队列。 |
| `./scripts/factor_learn.sh` | launchd/本地非交互学习 wrapper。 |
| `./scripts/factor_test.sh` | launchd/本地非交互测试 wrapper。 |

## Kronos 研究

| 命令 | 用途 |
|---|---|
| `./manage.py kronos-reference` | 生成研究用 Kronos 参考快照。 |
| `python3 kronos_us_experiment.py --mode latest` | 运行美股 Kronos 最小实验。 |
| `python3 kronos_confirmation_backtest.py` | 运行美股 Kronos 二次确认小回测。 |
| `python3 kronos_confirmation_backtest_a.py` | 运行 A 股 Kronos 二次确认小回测。 |

## 自动化

| 文件 | 用途 |
|---|---|
| `run_advisor_daily.sh` | 工作日生成策略推荐历史。 |
| `run_option_monitor.sh` | 每小时更新期权持仓、页面片段和本地真实盘观察。 |
| `auto_hedge_daily.sh` | 模拟盘止盈单自动维护。 |
| `install_launchd.sh` | 安装或卸载本地 launchd 任务。 |

## 注意事项

- 页面展示的是研究和观察信号，不是自动下单指令。
- 因子候选进入 `trial` 后才会影响运行时 overlay，仍需定期复测。
- 期权历史回测如果没有真实历史期权链和 IV，只能作为代理结果，不能作为正式胜率。
- 在当前 Desktop 路径下，launchd 可能遇到 macOS TCC 权限限制；长期建议迁移到 `~/Projects/9，主力分析`。
