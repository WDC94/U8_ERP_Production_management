# api/user_api.py
"""
用户相关RESTful API蓝图
- 提供注册、登录、用户查询、启用/禁用等接口
- 所有路由均以 /api/user/ 前缀对外
- 支持多账套操作（可接收account_code参数）
"""

from flask import Blueprint, request, jsonify
from modules.user import (
    create_user,
    check_user_login,
    get_user_by_username,
    set_user_active,
    list_users
)

user_api = Blueprint('user_api', __name__)

@user_api.route('/register', methods=['POST'])
def register():
    """
    用户注册接口
    请求JSON示例：
    {
        "username": "admin",
        "password": "123456",
        "realname": "管理员",
        "role": "admin",
        "remark": "超级管理员",
        "account_code": "022"
    }
    返回：{"msg": "...", "user": {...}}
    """
    data = request.json
    try:
        user = create_user(
            username=data['username'],
            password=data['password'],
            realname=data.get('realname', ''),
            role=data.get('role', 'user'),
            remark=data.get('remark'),
            account_code=data.get('account_code')
        )
        return jsonify({
            "msg": "用户注册成功",
            "user": {
                "id": user.id,
                "username": user.username,
                "realname": user.realname,
                "role": user.role,
                "is_active": user.is_active
            }
        })
    except Exception as e:
        return jsonify({"msg": f"注册失败: {str(e)}"}), 500

@user_api.route('/login', methods=['POST'])
def login():
    """
    用户登录接口
    请求JSON示例：
    {
        "username": "admin",
        "password": "123456",
        "account_code": "022"
    }
    返回：{"msg": "...", "user": {...}}
    """
    data = request.json
    user = check_user_login(
        username=data['username'],
        password=data['password'],
        account_code=data.get('account_code')
    )
    if user:
        return jsonify({
            "msg": "登录成功",
            "user": {
                "id": user.id,
                "username": user.username,
                "realname": user.realname,
                "role": user.role,
                "is_active": user.is_active
            }
        })
    else:
        return jsonify({"msg": "用户名或密码错误，或账号被禁用"}), 401

@user_api.route('/list', methods=['GET'])
def get_user_list():
    """
    查询所有用户（支持account_code参数，GET或querystring传递）
    返回：用户信息列表
    """
    account_code = request.args.get('account_code')
    users = list_users(account_code)
    users_data = [
        {
            "id": u.id,
            "username": u.username,
            "realname": u.realname,
            "role": u.role,
            "is_active": u.is_active
        }
        for u in users
    ]
    return jsonify(users_data)

@user_api.route('/disable', methods=['POST'])
def disable_user():
    """
    禁用/启用用户接口
    请求JSON示例：
    {
        "username": "admin",
        "is_active": false,
        "account_code": "022"
    }
    返回：{"msg": "..."}
    """
    data = request.json
    set_user_active(
        username=data['username'],
        is_active=data.get('is_active', False),
        account_code=data.get('account_code')
    )
    return jsonify({"msg": "操作成功"})

@user_api.route('/detail/<username>', methods=['GET'])
def user_detail(username):
    """
    获取单个用户详细信息
    路径参数：用户名
    """
    account_code = request.args.get('account_code')
    user = get_user_by_username(username, account_code)
    if user:
        return jsonify({
            "id": user.id,
            "username": user.username,
            "realname": user.realname,
            "role": user.role,
            "is_active": user.is_active,
            "remark": user.remark
        })
    else:
        return jsonify({"msg": "用户不存在"}), 404
