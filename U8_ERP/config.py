# config.py
"""
全局配置文件
- 管理数据库连接参数、账套信息、多账套切换等
- 配合db/session.py统一提供数据库连接入口
"""

import os

# 主数据库服务器连接参数
DB_SERVER = os.getenv('DB_SERVER', '192.168.10.250')
DB_USER = os.getenv('DB_USER', 'sa')
DB_PWD = os.getenv('DB_PWD', 'zb@2022')

# 多账套数据库名映射
ACCOUNT_SETS = {
    '022': 'UFDATA_022_2023',
    '088': 'UFDATA_088_2022',
}

# 当前账套代码（可通过接口或配置动态切换）
CURRENT_ACCOUNT_CODE = os.getenv('CURRENT_ACCOUNT_CODE', '022')

# 根据账套代码返回数据库名
def get_db_name(account_code=None):
    """
    根据账套代码获取数据库名
    :param account_code: 账套代码（如'022'），不传则取当前配置
    :return: 数据库名字符串
    """
    code = account_code or CURRENT_ACCOUNT_CODE
    return ACCOUNT_SETS.get(code, list(ACCOUNT_SETS.values())[0])  # 默认第一个

# 拼接ODBC连接字符串
def get_conn_str(account_code=None):
    """
    生成指定账套的ODBC连接字符串
    :param account_code: 账套代码（如'022'）
    :return: ODBC连接字符串
    """
    db_name = get_db_name(account_code)
    return (
        f"DRIVER={{SQL Server}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={db_name};"
        f"UID={DB_USER};"
        f"PWD={DB_PWD}"
    )

# 日志目录配置
LOG_DIR = os.getenv('LOG_DIR', os.path.join(os.path.dirname(__file__), 'logs'))
# 调试模式
DEBUG_MODE = os.getenv('DEBUG_MODE', 'True').lower() == 'true'
