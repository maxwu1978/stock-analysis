"""同花顺i问财数据模块 — 基于 pywencai 的自然语言选股

用法:
  from fetch_wencai import query
  df = query('沪深300成分股')
  df = query('市值大于500亿, ROE大于15, 非ST')
  df = query('近20日涨幅前30的A股')

依赖: pywencai (需要 Node.js 运行环境执行签名JS)
"""

import time
import pandas as pd


def query(prompt: str, loop: bool = False, retry: int = 2, sleep: float = 1.0) -> pd.DataFrame:
    """自然语言选股, 返回 pandas DataFrame.

    参数:
        prompt: 自然语言查询, 例如 "沪深300成分股", "ROE>15且非ST"
        loop: True=翻页取全量, False=只取第一页 (通常100条, 够用)
        retry: 失败重试次数
        sleep: 重试间隔秒数

    返回:
        DataFrame, 常见列: 股票代码/股票简称/最新价/涨跌幅/总市值/ROE等.
        问财会根据 prompt 动态决定返回字段, 不固定.
    """
    import pywencai  # 延迟导入, 避免未装时阻塞其他模块
    last_err = None
    for attempt in range(retry + 1):
        try:
            df = pywencai.get(query=prompt, loop=loop)
            if df is not None and hasattr(df, "shape") and len(df) > 0:
                return df
        except Exception as e:
            last_err = e
        if attempt < retry:
            time.sleep(sleep)
    if last_err:
        raise RuntimeError(f"pywencai 查询失败: {prompt} | {last_err}")
    return pd.DataFrame()


def get_stock_pool(prompt: str = "沪深300成分股", max_n: int | None = None) -> list[str]:
    """获取自然语言筛选后的股票代码列表 (6位数字).

    用于替代 fetch_data.STOCKS 的硬编码列表, 支持动态选股池.
    """
    df = query(prompt)
    if df is None or df.empty:
        return []
    # 问财返回列名有时是 'code' (6位), 有时是 '股票代码' (含市场后缀)
    if "code" in df.columns:
        codes = df["code"].astype(str).str.zfill(6).tolist()
    elif "股票代码" in df.columns:
        codes = df["股票代码"].astype(str).str[:6].tolist()
    else:
        raise ValueError(f"无法识别代码列, 可用列: {list(df.columns)}")
    # 去重保序
    seen = set()
    out = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    if max_n is not None:
        out = out[:max_n]
    return out
