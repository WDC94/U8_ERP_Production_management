# modules/mold.py
# -*- coding: utf-8 -*-
"""
模具模块（按最新建表语句对齐）
- 支持：模具台账新增/更新/列表/详情/删除
- 下拉：返还方式(Mujufanhuan)、模具工艺(MJGYi)
- 映射：模具 <-> U8存货（MJWLDZhao）
- 附件：MoldAttachment（沿用你现有表）
"""

import os
import re
from datetime import datetime
from db.session import get_dst_connection

# 上传根目录（可调整绝对路径，建议放 uploads/mold/ 下）
UPLOAD_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../uploads/mold'))

# =========================
# 下拉/搜索
# =========================
def list_refund_methods():
    """返还方式下拉：Mujufanhuan(id, M_fangshi)"""
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, M_fangshi FROM Mujufanhuan ORDER BY id")
        return [{'id': r[0], 'name': (r[1] or '').strip()} for r in cur.fetchall()]

def list_process_methods():
    """工艺下拉：MJGYi(id, MJ_gongyi)"""
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, MJ_gongyi FROM MJGYi ORDER BY id")
        return [{'id': r[0], 'name': (r[1] or '').strip()} for r in cur.fetchall()]

def search_supplier(keyword: str):
    """模糊搜索供应商"""
    if not keyword:
        return []
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, supplier_name FROM Supplier WHERE supplier_name LIKE ?", f"%{keyword}%")
        return [{'id': r[0], 'supplier_name': r[1]} for r in cur.fetchall()]

def search_inventory(keyword: str):
    """物料编码精确 / 名称模糊"""
    if not keyword:
        return []
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT cInvCode, cInvName
            FROM Inventory
            WHERE cInvCode = ? OR cInvName LIKE ?
            ORDER BY cInvCode
        """, keyword, f"%{keyword}%")
        return [{'cInvCode': r[0], 'cInvName': r[1]} for r in cur.fetchall()]

# =========================
# 工具
# =========================
def _table_has_columns(cur, table, cols):
    """容错：检查表是否存在这些列（用于与历史库兼容）"""
    cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", table)
    exists = {row[0].lower() for row in cur.fetchall()}
    return {c: (c.lower() in exists) for c in cols}

def _get_refund_name_by_id(cur, rid):
    if not rid:
        return None
    cur.execute("SELECT M_fangshi FROM Mujufanhuan WHERE id=?", rid)
    r = cur.fetchone()
    return (r[0].strip() if r and r[0] else None)

def _get_process_name_by_id(cur, pid):
    if not pid:
        return None
    cur.execute("SELECT MJ_gongyi FROM MJGYi WHERE id=?", pid)
    r = cur.fetchone()
    return (r[0].strip() if r and r[0] else None)

def _parse_remark_meta(remark_text):
    """历史兼容：解析 remark 中的 [公司=xxx][工艺=yyy]（如早期用过该写法）"""
    meta = {'company': '', 'process': ''}
    if not remark_text:
        return meta
    m1 = re.search(r'\[公司=(.*?)\]', remark_text)
    m2 = re.search(r'\[工艺=(.*?)\]', remark_text)
    if m1:
        meta['company'] = m1.group(1)
    if m2:
        meta['process'] = m2.group(1)
    return meta

def _save_attachments_to_db(cursor, mold_id, attachments):
    """保存附件到磁盘并入库（沿用既有 MoldAttachment 表）"""
    upload_dir = os.path.join(UPLOAD_ROOT, str(mold_id))
    os.makedirs(upload_dir, exist_ok=True)
    for file_item in attachments:
        if hasattr(file_item, 'filename'):
            filename = file_item.filename
            fileobj = file_item.stream
        else:
            filename, fileobj = file_item
        safe_filename = str(filename).replace('\\', '_').replace('/', '_')
        file_path = os.path.join(upload_dir, safe_filename)
        with open(file_path, 'wb') as f:
            f.write(fileobj.read() if hasattr(fileobj, 'read') else fileobj)
        cursor.execute(
            "INSERT INTO MoldAttachment (mold_id, file_name, file_path, upload_time) VALUES (?,?,?,?)",
            mold_id, safe_filename, file_path, datetime.now()
        )

def _replace_mold_materials(cur, mold_id, product_name, material_codes):
    """维护 MJWLDZhao（模具-存货 对照）为全量替换"""
    cur.execute("DELETE FROM MJWLDZhao WHERE MJ_id=?", str(mold_id))
    if not material_codes:
        return
    for code in material_codes:
        code = (code or '').strip()
        if not code:
            continue
        cur.execute("SELECT cInvName FROM Inventory WHERE cInvCode=?", code)
        r = cur.fetchone()
        name = r[0] if r else ''
        cur.execute(
            "INSERT INTO MJWLDZhao(MJ_id, MJ_name, cinvcode, cinvname) VALUES (?,?,?,?)",
            str(mold_id), product_name or '', code, name or ''
        )

# =========================
# 新增 / 更新
# =========================
def add_mold_period_v2(
    product_name,
    casting_supplier_id,
    mold_supplier_id,
    amount,
    refund_method_id,   # 新：返还方式id -> Mold.MujufanhuanI_id
    process_id,         # 新：工艺id     -> Mold.MJGYi_id
    company,            # 新：所属公司    -> Mold.company
    start_date,
    end_date,
    remark,
    material_codes,
    attachments=None
):
    """
    新增模具台账（按最新表结构）
    - 同时写入：MujufanhuanI_id、MJGYi_id、company
    - 兼容保留 refund 文本（写入返还方式名称，便于直显）
    """
    if not casting_supplier_id or not mold_supplier_id:
        return {'success': False, 'msg': '请选择供应商'}

    with get_dst_connection() as conn:
        cur = conn.cursor()

        refund_name = _get_refund_name_by_id(cur, refund_method_id) if refund_method_id else None
        process_name = _get_process_name_by_id(cur, process_id) if process_id else None

        # 为兼容极端环境，检查列是否存在（老库可能还没加上）
        cols = _table_has_columns(cur, 'Mold', ['MujufanhuanI_id', 'MJGYi_id', 'company', 'refund'])

        fields = [
            "product_name", "casting_supplier_id", "mold_supplier_id",
            "amount", "start_date", "end_date", "remark", "create_time"
        ]
        values = [
            product_name, int(casting_supplier_id), int(mold_supplier_id),
            float(amount) if amount else None, start_date, end_date, remark, datetime.now()
        ]

        # refund 文本字段：写入返还方式名称（也可不写）
        if cols.get('refund'):
            fields.append("refund"); values.append(refund_name or '')

        # 新增字段：返还方式id、工艺id、公司
        if cols.get('MujufanhuanI_id'):
            fields.append("MujufanhuanI_id"); values.append(int(refund_method_id) if refund_method_id else None)
        if cols.get('MJGYi_id'):
            fields.append("MJGYi_id"); values.append(int(process_id) if process_id else None)
        if cols.get('company'):
            fields.append("company"); values.append(company or None)

        sql = f"INSERT INTO Mold ({','.join(fields)}) VALUES ({','.join(['?']*len(fields))})"
        cur.execute(sql, *values)

        cur.execute("SELECT @@IDENTITY")
        mold_id = int(cur.fetchone()[0])

        # 1:N 物料映射
        _replace_mold_materials(cur, mold_id, product_name, material_codes)

        # 附件
        if attachments:
            _save_attachments_to_db(cur, mold_id, attachments)

        conn.commit()
        return {'success': True, 'mold_id': mold_id}

def update_mold_period_v2(
    mold_id,
    product_name,
    casting_supplier_id,
    mold_supplier_id,
    amount,
    refund_method_id,   # -> 更新 Mold.MujufanhuanI_id
    process_id,         # -> 更新 Mold.MJGYi_id
    company,            # -> 更新 Mold.company
    start_date,
    end_date,
    remark,
    material_codes
):
    """
    更新模具台账（按最新表结构）
    - 返还方式：更新 MujufanhuanI_id，并把名称写入 refund（可选）
    - 工艺：更新 MJGYi_id
    - 公司：更新 company
    """
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cols = _table_has_columns(cur, 'Mold', ['MujufanhuanI_id', 'MJGYi_id', 'company', 'refund'])

        refund_name = _get_refund_name_by_id(cur, refund_method_id) if refund_method_id else None

        sets, vals = [], []

        if product_name is not None:
            sets.append("product_name=?"); vals.append(product_name)
        if casting_supplier_id:
            sets.append("casting_supplier_id=?"); vals.append(int(casting_supplier_id))
        if mold_supplier_id:
            sets.append("mold_supplier_id=?"); vals.append(int(mold_supplier_id))
        if amount is not None:
            sets.append("amount=?"); vals.append(float(amount))
        if start_date is not None:
            sets.append("start_date=?"); vals.append(start_date)
        if end_date is not None:
            sets.append("end_date=?"); vals.append(end_date)
        if remark is not None:
            sets.append("remark=?"); vals.append(remark)

        # refund 文本字段与返还方式ID
        if cols.get('refund') and refund_name is not None:
            sets.append("refund=?"); vals.append(refund_name)
        if cols.get('MujufanhuanI_id') and refund_method_id is not None:
            sets.append("MujufanhuanI_id=?"); vals.append(int(refund_method_id))

        # 工艺ID
        if cols.get('MJGYi_id') and process_id is not None:
            sets.append("MJGYi_id=?"); vals.append(int(process_id))

        # 公司
        if cols.get('company') and company is not None:
            sets.append("company=?"); vals.append(company)

        if sets:
            sql = f"UPDATE Mold SET {', '.join(sets)} WHERE id=?"
            vals.append(int(mold_id))
            cur.execute(sql, *vals)

        # 替换 1:N 物料映射
        _replace_mold_materials(cur, mold_id, product_name, material_codes)

        conn.commit()
        return {'success': True}

# =========================
# 列表 / 详情
# =========================
def list_mold_period_v2():
    """
    列表：联表带出 返还方式名/工艺名/公司，并汇总映射的物料编码列表
    """
    data = []
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT  m.id,
                    m.product_name,
                    s1.supplier_name,            -- 铸造供应商
                    s2.supplier_name,            -- 模具供应商
                    m.start_date,
                    m.end_date,
                    m.amount,
                    m.refund,                    -- 文本（可能为空）
                    m.remark,
                    m.company,
                    r.M_fangshi,                 -- 返还方式名
                    g.MJ_gongyi                  -- 工艺名
            FROM Mold m
            LEFT JOIN Supplier s1 ON m.casting_supplier_id = s1.id
            LEFT JOIN Supplier s2 ON m.mold_supplier_id   = s2.id
            LEFT JOIN Mujufanhuan r ON m.MujufanhuanI_id  = r.id
            LEFT JOIN MJGYi       g ON m.MJGYi_id         = g.id
            ORDER BY m.id DESC
        """)
        rows = cur.fetchall()
        if not rows:
            return []

        ids = [str(r[0]) for r in rows]

        # 取 1:N 物料编码映射
        mp = {}
        if ids:
            placeholders = ",".join(["?"] * len(ids))
            cur.execute(f"SELECT MJ_id, cinvcode FROM MJWLDZhao WHERE MJ_id IN ({placeholders})", *ids)
            for mj_id, code in cur.fetchall():
                mp.setdefault(mj_id, []).append(code)

        for r in rows:
            mold_id = r[0]
            # remark 如果历史里写过 [公司=][工艺=] 也做个解析（仅兜底）
            remark_meta = _parse_remark_meta(r[8])
            data.append({
                'mold_id': mold_id,
                'product_name': r[1] or '',
                'casting_supplier': r[2] or '',
                'mold_supplier': r[3] or '',
                'materials': ", ".join(mp.get(str(mold_id), [])),
                'start_date': r[4] or '',
                'end_date': r[5] or '',
                'amount': r[6] if r[6] is not None else '',
                'refund': (r[7] or r[10] or ''),  # 优先文本refund，否则取返还方式名
                'process': (r[11] or remark_meta.get('process', '')),
                'company': (r[9] or remark_meta.get('company', '')),
                'remark': r[8] or ''
            })
    return data

def get_mold_period_v2(mold_id: int):
    """
    详情：联表带出 返还方式（id+name）、工艺（id+name）、公司、映射存货、附件
    """
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT  m.id,
                    m.product_name,
                    m.casting_supplier_id,
                    m.mold_supplier_id,
                    m.amount,
                    m.refund,
                    m.start_date,
                    m.end_date,
                    m.remark,
                    s1.supplier_name,          -- 铸造供应商名
                    s2.supplier_name,          -- 模具供应商名
                    m.MujufanhuanI_id,         -- 返还方式id
                    r.M_fangshi,               -- 返还方式名
                    m.MJGYi_id,                -- 工艺id
                    g.MJ_gongyi,               -- 工艺名
                    m.company,                 -- 所属公司
                    m.create_time
            FROM Mold m
            LEFT JOIN Supplier   s1 ON m.casting_supplier_id = s1.id
            LEFT JOIN Supplier   s2 ON m.mold_supplier_id   = s2.id
            LEFT JOIN Mujufanhuan r ON m.MujufanhuanI_id    = r.id
            LEFT JOIN MJGYi       g ON m.MJGYi_id           = g.id
            WHERE m.id = ?
        """, mold_id)
        r = cur.fetchone()
        if not r:
            return None

        # 物料清单（对照关系）
        cur.execute("SELECT cinvcode, cinvname FROM MJWLDZhao WHERE MJ_id=?", str(mold_id))
        mats = [{'cInvCode': rr[0], 'cInvName': rr[1]} for rr in cur.fetchall()]

        return {
            'mold_id': r[0],
            'product_name': r[1] or '',
            'casting_supplier_id': r[2],
            'mold_supplier_id': r[3],
            'amount': r[4] if r[4] is not None else '',
            'refund': r[5] or '',                 # 文本
            'start_date': r[6] or '',
            'end_date': r[7] or '',
            'remark': r[8] or '',
            'casting_supplier': r[9] or '',
            'mold_supplier': r[10] or '',
            'refund_method_id': r[11],
            'refund_method_name': r[12] or '',
            'process_id': r[13],
            'process_name': r[14] or '',
            'company': r[15] or '',
            'create_time': r[16] or '',
            'materials': mats,
            'attachments': get_attachments(mold_id)
        }

# =========================
# 附件
# =========================
def get_attachments(mold_id):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, file_name, file_path, upload_time FROM MoldAttachment WHERE mold_id=? ORDER BY id", mold_id)
        res = []
        for row in cur.fetchall():
            res.append({
                'id': row[0],
                'file_name': row[1],
                'file_path': row[2],
                'upload_time': row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else ''
            })
    return res

def get_attachments_by_id(attachment_id):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT file_name, file_path FROM MoldAttachment WHERE id=?", attachment_id)
        row = cur.fetchone()
        if not row:
            return None
        return {'file_name': row[0], 'file_path': row[1]}

def delete_attachment(attachment_id):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT file_path FROM MoldAttachment WHERE id=?", attachment_id)
        row = cur.fetchone()
        if not row:
            return {'success': False, 'msg': '未找到附件'}
        file_path = row[0]
        cur.execute("DELETE FROM MoldAttachment WHERE id=?", attachment_id)
        conn.commit()
    if file_path and os.path.isfile(file_path):
        try:
            os.remove(file_path)
        except Exception:
            pass
    return {'success': True}

# =========================
# 删除
# =========================
def delete_mold_period(mold_id):
    """存在调拨记录不可删；清理附件与映射"""
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM MoldTransferRecord WHERE mold_id=?", mold_id)
        if cur.fetchone()[0] > 0:
            return {'success': False, 'msg': '该模具存在调拨记录，无法删除！'}

        # 附件
        cur.execute("SELECT id FROM MoldAttachment WHERE mold_id=?", mold_id)
        for row in cur.fetchall():
            delete_attachment(row[0])

        # 物料映射
        cur.execute("DELETE FROM MJWLDZhao WHERE MJ_id=?", str(mold_id))

        # 主表
        cur.execute("DELETE FROM Mold WHERE id=?", mold_id)
        conn.commit()

    # 清空目录（若为空）
    mold_dir = os.path.join(UPLOAD_ROOT, str(mold_id))
    if os.path.isdir(mold_dir):
        try:
            os.rmdir(mold_dir)
        except Exception:
            pass
    return {'success': True}
