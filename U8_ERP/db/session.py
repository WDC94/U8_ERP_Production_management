# db/session.py
"""
数据库会话管理
- 统一管理与SQL Server的连接，支持多账套动态切换
- 对外只暴露 get_connection 方法，避免重复造轮子
- 后续业务模块需数据库操作时，只需 import 并调用 get_connection
"""

import pyodbc
from config import get_conn_str, CURRENT_ACCOUNT_CODE

def get_connection(account_code=None):
    """
    获取数据库连接（支持多账套切换）
    :param account_code: 指定账套代码（如'022'），不传则取当前账套
    :return: pyodbc.Connection 对象
    用法示例：
        conn = get_connection()                # 用当前账套
        conn2 = get_connection('088')          # 用088账套
    """
    conn_str = get_conn_str(account_code)
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        # 建议可在此扩展日志记录
        raise RuntimeError(f"数据库连接失败: {e}")

