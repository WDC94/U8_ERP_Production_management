# modules/user.py
"""
用户管理业务模块
- 提供用户注册、登录验证、用户信息查询、用户启用/禁用等功能
- 所有操作均通过SQLAlchemy ORM操作数据库
- 支持多账套数据库（如有需求可传入account_code参数）
"""

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from db.models import SysUser
from config import get_conn_str, CURRENT_ACCOUNT_CODE

def get_engine(account_code=None):
    """
    获取SQLAlchemy数据库引擎，支持多账套切换
    :param account_code: 账套代码，如'022'，不传则用当前默认账套
    :return: SQLAlchemy engine对象
    """
    conn_str = get_conn_str(account_code)
    # pyodbc支持的SQLAlchemy连接字符串写法
    sqlalchemy_url = f"mssql+pyodbc:///?odbc_connect={conn_str.replace(';', '%3B')}"
    return create_engine(sqlalchemy_url, echo=False, future=True)

def get_session(account_code=None):
    """
    获取数据库ORM会话
    :param account_code: 账套代码
    :return: session对象
    """
    engine = get_engine(account_code)
    Session = sessionmaker(bind=engine)
    return Session()

def get_user_by_username(username, account_code=None):
    """
    根据用户名查找用户
    :param username: 用户名
    :param account_code: 账套代码
    :return: SysUser对象或None
    """
    session = get_session(account_code)
    user = session.query(SysUser).filter(SysUser.username == username).first()
    session.close()
    return user

def create_user(username, password, realname='', role='user', remark=None, account_code=None):
    """
    新建用户
    :param username: 用户名
    :param password: 密码
    :param realname: 真实姓名
    :param role: 角色
    :param remark: 备注
    :param account_code: 账套代码
    :return: 创建的SysUser对象
    """
    session = get_session(account_code)
    user = SysUser(
        username=username,
        password=password,
        realname=realname,
        role=role,
        remark=remark
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    session.close()
    return user

def check_user_login(username, password, account_code=None):
    """
    登录验证
    :param username: 用户名
    :param password: 密码
    :param account_code: 账套代码
    :return: SysUser对象或None
    """
    user = get_user_by_username(username, account_code)
    if user and user.password == password and user.is_active:
        return user
    else:
        return None

def set_user_active(username, is_active=True, account_code=None):
    """
    启用/禁用用户
    :param username: 用户名
    :param is_active: True为启用，False为禁用
    :param account_code: 账套代码
    """
    session = get_session(account_code)
    user = session.query(SysUser).filter(SysUser.username == username).first()
    if user:
        user.is_active = is_active
        session.commit()
    session.close()

def list_users(account_code=None):
    """
    查询所有用户
    :param account_code: 账套代码
    :return: 用户对象列表
    """
    session = get_session(account_code)
    users = session.query(SysUser).all()
    session.close()
    return users
