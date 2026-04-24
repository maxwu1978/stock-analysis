# 工程整理执行记录

最后更新：2026-04-24

## 当前基线

- 稳定线上版本：`41613d0 Add retail sentiment risk overlay`
- 稳定标签：`stable-retail-overlay-20260424`
- 线上页面：GitHub Pages 已发布，`CN Retail` 与散户情绪标记可见
- 当前策略：先整理操作入口和生成物治理，不移动核心源码，不改包结构

## 已执行

### 1. 生成物治理

- 扩展 `.gitignore`，忽略回测 CSV、临时报表、日志、本地自动化状态和缓存。
- 未跟踪文件数量从约 `193` 降到约 `49`，剩余主要是新增源码、脚本和历史计划文件。
- 暂未对已跟踪的 CSV/JSON 做 `git rm --cached`，避免误删仍被系统引用的基准文件。

### 2. 统一命令入口

新增 `manage.py`，保留原脚本兼容入口，只做 subprocess 分发：

```bash
python3 manage.py refresh-page
python3 manage.py smoke-test
python3 manage.py smoke-test --remote
python3 manage.py scan-cn --days 800
python3 manage.py refresh-options
python3 manage.py factor-learn --duration-min 60
python3 manage.py factor-test
python3 manage.py preflight
```

新增 `scripts/preflight.sh`，用于上线前固定检查：

```bash
./scripts/preflight.sh
```

当前检查内容：

- 编译顶层 Python 脚本
- 本地页面烟测
- `git diff --check`

### 3. 自动化入口统一

- 本地 wrapper 已验证可以转发到 `manage.py factor-learn` / `manage.py factor-test`。
- 这些 wrapper 应与 `factor_learning.py` / `factor_testing.py` / `factor_idea_generator.py` 等因子学习源码同批入库，避免干净克隆后命令入口存在但实现脚本缺失。
- 当前第一批整理只固定通用入口和发布前检查；因子学习源码归入第二批审查。

### 4. 项目入口文档化

- 新增 `README.md`，作为根目录快速入口，明确线上页面、统一命令、发布检查和工程边界。
- 新增 `COMMANDS.md`，把页面发布、A 股验证、行业热度、期权研究、因子学习、Kronos 和自动化脚本按用途分组。
- 扩展 `manage.py list-commands`，让本地可直接查看当前稳定命令表。

### 5. 研究工具接入统一命令

以下工具已保留原脚本名，并接入 `manage.py`：

- `validate-a` → `a_share_signal_validation_2y.py`
- `capital-flow-backtest` → `cn_capital_flow_backtest.py`
- `industry-heat` → `industry_heat.py`
- `option-signal-review` → `historical_option_signal_review.py`
- `option-pnl-review` → `historical_option_pnl_review.py`
- `option-account-sim` → `historical_option_account_sim.py`
- `import-option-chains` → `import_option_chain_data.py`
- `fetch-option-chains` → `fetch_dolthub_option_chains.py`
- `kronos-reference` → `build_kronos_reference.py`

本阶段仍不把研究脚本移动到 package 目录。原因是当前很多脚本存在裸 import、根目录默认输出和 GitHub Actions/launchd 固定路径，直接搬迁风险高。

### 6. 自动化路径收口

- `run_advisor_daily.sh` / `run_option_monitor.sh` / `auto_hedge_daily.sh` 已从旧的 `/Volumes/MaxRelocated/主力分析` 硬编码改为按脚本所在目录定位项目。
- 三个脚本均保留 `venv/bin/python` 优先、否则回退 `python3` 的运行方式。
- 新增 `manage.py doctor`，检查核心入口文件、shell wrapper 可执行位、launchd plist 语法和核心脚本旧路径硬编码。
- `scripts/refresh_option_strategy.sh` 与 `scripts/scan_a_opportunities.sh` 已转为调用 `manage.py`，减少重复入口。

### 7. GitHub Actions 入口统一

- `update-page.yml` 已改为调用 `manage.py refresh-us-quotes` 和 `manage.py refresh-page --strict`。
- `factor-lab.yml` 已改为调用 `manage.py factor-ideas` 和 `manage.py factor-promotion`。
- `scripts/run_factor_lab_ci.sh` 已改为调用 `manage.py factor-lab`。
- `run_option_monitor.sh` 与 `run_factor_testing_daily.sh` 内部页面再生成已改为 `manage.py refresh-page --strict`。
- `refresh_us_quotes_page.py` 已补标准 `argparse` 入口，避免 `--help` 被误当成刷新执行。

## 已确认问题

### launchd 因子任务 `Operation not permitted`

根因不是 plist 格式、执行位或脚本语法，而是 macOS TCC 隐私限制：

- 项目位于 `~/Desktop/...`
- launchd 后台任务通过 `/bin/bash` 读取 Desktop 下脚本时被系统策略拒绝
- 日志表现为 `Operation not permitted`，退出码 `126`

推荐修复：

```bash
cd "/Users/wuqingxin/Desktop/test/Program/9，主力分析"
./install_launchd.sh uninstall-factors

mkdir -p "/Users/wuqingxin/Projects"
mv "/Users/wuqingxin/Desktop/test/Program/9，主力分析" "/Users/wuqingxin/Projects/9，主力分析"

cd "/Users/wuqingxin/Projects/9，主力分析"
./install_launchd.sh install-factors
```

备选方案是给 `/bin/bash` 和 Python 解释器 Full Disk Access，但权限面更大，不推荐作为首选。

## 暂不执行

- 不移动顶层 Python 文件到 package 目录，因为当前大量脚本使用裸 import。
- 不移动根目录已有 CSV/JSON 到 `reports/`，因为部分脚本仍默认读写根目录。
- 不提交历史实验文件，先保持本地可见，后续再按主题纳入版本控制。

## 下一步建议

1. 迁移项目到 `~/Projects/9，主力分析`，重新安装 launchd 因子任务。
2. 第二批清理已跟踪生成物：对确认不再作为基准输入的 CSV/JSON 执行 `git rm --cached`。
3. 后续再考虑 package 化；在此之前只增加统一入口，不移动核心脚本。
