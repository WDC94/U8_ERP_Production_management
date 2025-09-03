# -*- coding: utf-8 -*-
"""
api/data_setting_api.py
数据设置模块 - Flask 接口层（Blueprint）

配合 app.py 使用：
    from api.data_setting_api import data_setting_bp
    app.register_blueprint(data_setting_bp, url_prefix='/api')

前端请求路径示例（与本文件一一对应）：
- 用户：
  GET    /api/users/list?kw=
  POST   /api/users
  PUT    /api/users/<id>
  DELETE /api/users/<id>

- 返还方式（Mujufanhuan）：
  GET    /api/mold/dictionary/refund_methods
  POST   /api/mold/dictionary/refund_methods      {name}
  PUT    /api/mold/dictionary/refund_methods/<id> {name}
  DELETE /api/mold/dictionary/refund_methods/<id>

- 工艺（MJGYi）：
  GET    /api/mold/dictionary/process_methods
  POST   /api/mold/dictionary/process_methods      {name}
  PUT    /api/mold/dictionary/process_methods/<id> {name}
  DELETE /api/mold/dictionary/process_methods/<id>

- 模具与物料对照（MJWLDZhao）：
  GET    /api/mold/mjwldzhao/list?kw=&page=&size=
"""


from flask import Blueprint, request, jsonify
from modules.data_setting import DataSettingService


data_setting_api = Blueprint("data_setting", __name__)
svc = DataSettingService()

# ---------------- 用户 ----------------
@data_setting_api.route("/users/list", methods=["GET"])
def users_list():
    try:
        kw = (request.args.get("kw") or "").strip()
        rows = svc.list_users(kw=kw)
        return jsonify({"success": True, "data": rows})
    except Exception as e:
        return jsonify({"success": False, "msg": f"查询失败: {e}"}), 500


@data_setting_api.route("/users", methods=["POST"])
def users_create():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        new_id = svc.create_user(payload)
        return jsonify({"success": True, "id": new_id})
    except Exception as e:
        return jsonify({"success": False, "msg": f"创建失败: {e}"}), 400


@data_setting_api.route("/users/<int:uid>", methods=["PUT"])
def users_update(uid: int):
    try:
        payload = request.get_json(force=True, silent=True) or {}
        svc.update_user(uid, payload)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "msg": f"更新失败: {e}"}), 400


@data_setting_api.route("/users/<int:uid>", methods=["DELETE"])
def users_delete(uid: int):
    try:
        svc.delete_user(uid)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "msg": f"删除失败: {e}"}), 400


# ------------- 返还方式（Mujufanhuan） -------------
@data_setting_api.route("/mold/dictionary/refund_methods", methods=["GET"])
def refund_list():
    try:
        rows = svc.list_refund_methods()
        # 前端已兼容直接数组或 {data: []}，这里直接返回数组
        return jsonify(rows)
    except Exception as e:
        return jsonify({"success": False, "msg": f"查询失败: {e}"}), 500


@data_setting_api.route("/mold/dictionary/refund_methods", methods=["POST"])
def refund_create():
    try:
        name = (request.get_json(force=True, silent=True) or {}).get("name", "").strip()
        if not name:
            return jsonify({"success": False, "msg": "参数 name 不能为空"}), 400
        new_id = svc.create_refund_method(name)
        return jsonify({"success": True, "id": new_id})
    except Exception as e:
        return jsonify({"success": False, "msg": f"创建失败: {e}"}), 400


@data_setting_api.route("/mold/dictionary/refund_methods/<int:rid>", methods=["PUT"])
def refund_update(rid: int):
    try:
        name = (request.get_json(force=True, silent=True) or {}).get("name", "").strip()
        if not name:
            return jsonify({"success": False, "msg": "参数 name 不能为空"}), 400
        svc.update_refund_method(rid, name)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "msg": f"更新失败: {e}"}), 400


@data_setting_api.route("/mold/dictionary/refund_methods/<int:rid>", methods=["DELETE"])
def refund_delete(rid: int):
    try:
        svc.delete_refund_method(rid)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "msg": f"删除失败: {e}"}), 400


# ---------------- 工艺（MJGYi） ----------------
@data_setting_api.route("/mold/dictionary/process_methods", methods=["GET"])
def proc_list():
    try:
        rows = svc.list_process_methods()
        return jsonify(rows)  # 直接数组
    except Exception as e:
        return jsonify({"success": False, "msg": f"查询失败: {e}"}), 500


@data_setting_api.route("/mold/dictionary/process_methods", methods=["POST"])
def proc_create():
    try:
        name = (request.get_json(force=True, silent=True) or {}).get("name", "").strip()
        if not name:
            return jsonify({"success": False, "msg": "参数 name 不能为空"}), 400
        new_id = svc.create_process_method(name)
        return jsonify({"success": True, "id": new_id})
    except Exception as e:
        return jsonify({"success": False, "msg": f"创建失败: {e}"}), 400


@data_setting_api.route("/mold/dictionary/process_methods/<int:pid>", methods=["PUT"])
def proc_update(pid: int):
    try:
        name = (request.get_json(force=True, silent=True) or {}).get("name", "").strip()
        if not name:
            return jsonify({"success": False, "msg": "参数 name 不能为空"}), 400
        svc.update_process_method(pid, name)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "msg": f"更新失败: {e}"}), 400


@data_setting_api.route("/mold/dictionary/process_methods/<int:pid>", methods=["DELETE"])
def proc_delete(pid: int):
    try:
        svc.delete_process_method(pid)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "msg": f"删除失败: {e}"}), 400


# -------- 模具与物料对照（分页） --------
@data_setting_api.route("/mold/mjwldzhao/list", methods=["GET"])
def mjwldzhao_list():
    try:
        kw = (request.args.get("kw") or "").strip()
        page = int(request.args.get("page") or 1)
        size = int(request.args.get("size") or 20)
        rows, total = svc.list_mjwldzhao(kw=kw, page=page, size=size)
        return jsonify({"success": True, "data": rows, "total": total})
    except Exception as e:
        return jsonify({"success": False, "msg": f"查询失败: {e}"}), 500
