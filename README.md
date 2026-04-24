# 主力分析项目

这是一个本地优先的股票、期权和因子研究系统。公开输出通过 GitHub Pages 展示，交易相关能力默认保持观察、模拟或只读，不能把研究信号直接等同于真实交易指令。

## 当前入口

- 线上页面: <https://maxwu1978.github.io/stock-analysis/>
- 本地页面源码: `docs/`
- 统一命令入口: `./manage.py`
- 命令索引: `COMMANDS.md`
- 当前状态记录: `PROJECT_STATUS.md`
- 工程整理记录: `ENGINEERING_CLEANUP_PLAN.md`
- 因子学习机制: `FACTOR_DISCOVERY_PLAN.md`

## 常用命令

```bash
./manage.py list-commands
./manage.py refresh-page
./manage.py smoke-test
./manage.py smoke-test --remote
./manage.py preflight
```

A 股机会和可靠性验证：

```bash
./manage.py scan-cn
./manage.py validate-a --years 2
./manage.py capital-flow-backtest
./manage.py industry-heat --focus us-ai
```

期权研究和真实期权链数据：

```bash
./manage.py refresh-options
./manage.py option-signal-review --period 2y
./manage.py option-pnl-review --execution-filter
./manage.py option-account-sim --initial-capital 10000
./manage.py import-option-chains --input vendor_option_chain.csv
./manage.py fetch-option-chains --max-targets 10
```

因子学习和测试：

```bash
./scripts/factor_learn.sh
./scripts/factor_test.sh
./manage.py factor-learn --duration-min 60
./manage.py factor-test
```

## 发布前检查

上线或推送前至少执行：

```bash
./manage.py preflight
```

该检查会编译顶层 Python 脚本、执行本地页面 smoke test，并运行 `git diff --check`。如果要同时检查线上 GitHub Pages：

```bash
./manage.py preflight --remote
```

## 工程边界

- 不把公开页面上的信号自动解释为下单指令。
- 不自动切真实盘交易；富途交易脚本必须保持模拟盘或显式确认约束。
- 不把候选因子直接写入主模型核心因子列表；候选因子必须先经过学习、测试、trial 和晋升记录。
- 不轻易移动顶层脚本；当前大量脚本仍依赖裸 import 和固定路径。
- 运行生成的 CSV、日志、缓存和本地状态默认不入库，除非它们是明确的基准报告或页面输入。

## 目录约定

- `docs/`: GitHub Pages 多页输出。
- `scripts/`: 稳定 shell wrapper，主要转发到 `manage.py`。
- `.github/workflows/`: GitHub Actions 页面刷新和因子实验。
- `.cache/`: 本地测试、因子学习和临时实验缓存，不入库。
- 根目录 Python 脚本: 当前保留原位置，按 `COMMANDS.md` 做功能分组。

