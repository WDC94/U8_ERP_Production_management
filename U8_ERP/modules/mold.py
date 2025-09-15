# modules/mold.py

import os
import shutil
from datetime import datetime
from db.session import get_dst_connection  # 如果你的项目是 db/session.py，请改为: from db.session import get_dst_connection

# 上传根目录（可按需调整）
UPLOAD_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../uploads/mold'))

# ====================== 工具函数 ======================
def _col_exists(cur, table_name: str, col_name: str) -> bool:
    """检查列是否存在（SQL Server）"""
    cur.execute("""
        SELECT 1
        FROM sys.columns c
        JOIN sys.objects o ON o.object_id = c.object_id
        WHERE o.name = ? AND c.name = ?
    """, table_name, col_name)
    return cur.fetchone() is not None

def _to_bit(v):
    if v in (True, 1, '1', 'true', 'True', 'YES', 'yes', '是'):
        return 1
    if v in (False, 0, '0', 'false', 'False', 'NO', 'no', '否', None, ''):
        return 0
    try:
        return 1 if int(v) != 0 else 0
    except Exception:
        return 0

def _safe_float(v):
    try:
        return float(v) if v not in (None, '',) else None
    except Exception:
        return None

def _ensure_upload_dir(mold_id: int):
    d = os.path.join(UPLOAD_ROOT, str(mold_id))
    os.makedirs(d, exist_ok=True)
    return d

# ====================== 字典/下拉 ======================
def search_supplier(keyword):
    """模糊搜索供应商，返回 [{'id':..., 'supplier_name':...}, ...]"""
    if not keyword:
        return []
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, supplier_name FROM Supplier WHERE supplier_name LIKE ? ORDER BY supplier_name",
            f"%{keyword}%"
        )
        return [{'id': r[0], 'supplier_name': r[1]} for r in cur.fetchall()]

def search_inventory(keyword):
    """物料编码或名称精确/模糊查找"""
    if not keyword:
        return []
    kw = f"%{keyword}%"
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP (100) cInvCode, cInvName
            FROM Inventory
            WHERE cInvCode LIKE ? OR cInvName LIKE ?
            ORDER BY cInvCode
        """, kw, kw)
        return [{'cInvCode': row[0], 'cInvName': row[1]} for row in cur.fetchall()]

def list_refund_methods():
    with get_dst_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT id, name FROM RefundMethod ORDER BY id")
            return [{'id': r[0], 'name': r[1]} for r in cur.fetchall()]
        except Exception:
            return []

def list_process_methods():
    with get_dst_connection() as conn:
        cur = conn.cursor()
        for tbl in ('MoldProcess', 'ProcessMethod'):
            try:
                cur.execute(f"SELECT id, name FROM {tbl} ORDER BY id")
                return [{'id': r[0], 'name': r[1]} for r in cur.fetchall()]
            except Exception:
                continue
        return []

# ====================== 附件 ======================
def save_attachment(mold_id, file_storage):
    """保存单个附件（供上传接口调用）"""
    if not file_storage:
        return None
    filename = (file_storage.filename or '').strip()
    if not filename:
        return None

    upload_dir = _ensure_upload_dir(mold_id)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    safe_name = f"{ts}_{filename}"
    abs_path = os.path.join(upload_dir, safe_name)
    file_storage.save(abs_path)

    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO MoldAttachment (mold_id, file_name, file_path, upload_time) VALUES (?, ?, ?, ?)",
            mold_id, filename, abs_path, datetime.now()
        )
        cur.execute("SELECT @@IDENTITY")
        attach_id = int(cur.fetchone()[0])
        conn.commit()
    return {'id': attach_id, 'file_name': filename, 'file_path': abs_path}

def get_attachment_path(attach_id: int):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT file_path FROM MoldAttachment WHERE id=?", attach_id)
        row = cur.fetchone()
        return row[0] if row else None

def get_attachments_by_id(attach_id: int):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT file_name, file_path FROM MoldAttachment WHERE id=?", attach_id)
        row = cur.fetchone()
        if not row:
            return None
        return {'file_name': row[0], 'file_path': row[1]}

def delete_attachment(attachment_id: int):
    path = None
    with get_dst_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT file_path FROM MoldAttachment WHERE id=?", attachment_id)
        r = cur.fetchone()
        if r:
            path = r[0]
        cur.execute("DELETE FROM MoldAttachment WHERE id=?", attachment_id)
        conn.commit()
    if path and os.path.isfile(path):
        try:
            os.remove(path)
        except Exception:
            pass
    return {'success': True}

def save_attachments_to_db(cursor, mold_id, attachments):
    """批量保存附件（与 add/update 组合使用）"""
    upload_dir = _ensure_upload_dir(mold_id)
    for item in attachments or []:
        if hasattr(item, 'save'):  # FileStorage
            filename = (item.filename or '').strip()
            if not filename:
                continue
            ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            safe_name = f"{ts}_{filename}"
            abs_path = os.path.join(upload_dir, safe_name)
            item.save(abs_path)
            cursor.execute(
                "INSERT INTO MoldAttachment (mold_id, file_name, file_path, upload_time) VALUES (?, ?, ?, ?)",
                mold_id, filename, abs_path, datetime.now()
            )
        else:
            filename, fileobj = item
            if not filename:
                continue
            ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            safe_name = f"{ts}_{filename}"
            abs_path = os.path.join(upload_dir, safe_name)
            with open(abs_path, 'wb') as f:
                if hasattr(fileobj, 'read'):
                    shutil.copyfileobj(fileobj, f)
                else:
                    f.write(fileobj)
            cursor.execute(
                "INSERT INTO MoldAttachment (mold_id, file_name, file_path, upload_time) VALUES (?, ?, ?, ?)",
                mold_id, filename, abs_path, datetime.now()
            )

# ====================== 台账：新增（v2，含三新字段） ======================
def add_mold_period_v2(*, product_name, casting_supplier_id, mold_supplier_id,
                        amount=None, refund_method_id=None, process_id=None, company=None,
                        start_date=None, end_date=None, remark=None,
                        material_codes=None, attachments=None,
                        advance_amount=None, balance_unpaid=None, is_invoiced=None):
    # 基础校验
    if not product_name or not str(product_name).strip():
        return {'success': False, 'msg': '产品名称必填'}
    if not casting_supplier_id or not mold_supplier_id:
        return {'success': False, 'msg': '请选择供应商'}

    with get_dst_connection() as conn:
        cur = conn.cursor()

        # 列存在性检查
        has_adv = _col_exists(cur, 'Mold', 'advance_amount')
        has_bal = _col_exists(cur, 'Mold', 'balance_unpaid')
        has_inv = _col_exists(cur, 'Mold', 'is_invoiced')
        has_materials = _col_exists(cur, 'Mold', 'materials')
        has_material_code = _col_exists(cur, 'Mold', 'material_code')
        has_company = _col_exists(cur, 'Mold', 'company')
        has_process_id = _col_exists(cur, 'Mold', 'process_id')
        has_refund = _col_exists(cur, 'Mold', 'refund')
        has_create_time = _col_exists(cur, 'Mold', 'create_time')

        cols = ['product_name', 'casting_supplier_id', 'mold_supplier_id', 'amount', 'start_date', 'end_date', 'remark']
        vals = [product_name, int(casting_supplier_id), int(mold_supplier_id), _safe_float(amount), start_date, end_date, (remark or '')]

        if has_create_time:
            cols.append('create_time'); vals.append(datetime.now())

        # materials / material_code
        csv_materials = None
        if material_codes and isinstance(material_codes, (list, tuple)):
            csv_materials = ','.join(str(x).strip() for x in material_codes if str(x).strip())
        if has_materials and csv_materials is not None:
            cols.insert(1, 'materials'); vals.insert(1, csv_materials)
        elif has_material_code and csv_materials:
            cols.insert(1, 'material_code'); vals.insert(1, csv_materials.split(',')[0])

        # 公司/工艺
        if has_company:
            cols.append('company'); vals.append(company)
        if has_process_id:
            vals_proc = int(process_id) if process_id not in (None, '',) else None
            cols.append('process_id'); vals.append(vals_proc)

        # 返还方式（refund 存名称，列宽仅 NVARCHAR(4)，安全截断）
        if has_refund and refund_method_id not in (None, '',):
            try:
                cur.execute("SELECT name FROM RefundMethod WHERE id=?", int(refund_method_id))
                rr = cur.fetchone()
                refund_name = rr[0] if rr else None
            except Exception:
                refund_name = None
            cols.append('refund'); vals.append((refund_name or '')[:4])

        # 三个新字段
        if has_adv: cols.append('advance_amount'); vals.append(_safe_float(advance_amount))
        if has_bal: cols.append('balance_unpaid'); vals.append(_safe_float(balance_unpaid))
        if has_inv: cols.append('is_invoiced'); vals.append(_to_bit(is_invoiced))

        # INSERT
        placeholders = ', '.join(['?'] * len(cols))
        sql = f"INSERT INTO Mold ({', '.join(cols)}) VALUES ({placeholders})"
        cur.execute(sql, *vals)

        cur.execute("SELECT @@IDENTITY")
        mold_id = int(cur.fetchone()[0])

        # 附件
        if attachments:
            save_attachments_to_db(cur, mold_id, attachments)

        conn.commit()
    return {'success': True, 'mold_id': mold_id}

# ====================== 台账：更新（v2） ======================
def update_mold_period_v2(*, mold_id, product_name=None, casting_supplier_id=None, mold_supplier_id=None,
                           amount=None, refund_method_id=None, process_id=None, company=None,
                           start_date=None, end_date=None, remark=None,
                           material_codes=None,
                           advance_amount=None, balance_unpaid=None, is_invoiced=None):
    sets, params = [], []

    with get_dst_connection() as conn:
        cur = conn.cursor()
        has_adv = _col_exists(cur, 'Mold', 'advance_amount')
        has_bal = _col_exists(cur, 'Mold', 'balance_unpaid')
        has_inv = _col_exists(cur, 'Mold', 'is_invoiced')
        has_materials = _col_exists(cur, 'Mold', 'materials')
        has_material_code = _col_exists(cur, 'Mold', 'material_code')
        has_company = _col_exists(cur, 'Mold', 'company')
        has_process_id = _col_exists(cur, 'Mold', 'process_id')
        has_refund = _col_exists(cur, 'Mold', 'refund')

        if product_name is not None: sets.append('product_name=?'); params.append(product_name)
        if casting_supplier_id is not None: sets.append('casting_supplier_id=?'); params.append(int(casting_supplier_id))
        if mold_supplier_id is not None: sets.append('mold_supplier_id=?'); params.append(int(mold_supplier_id))
        if amount is not None: sets.append('amount=?'); params.append(_safe_float(amount))
        if start_date is not None: sets.append('start_date=?'); params.append(start_date)
        if end_date is not None: sets.append('end_date=?'); params.append(end_date)
        if remark is not None: sets.append('remark=?'); params.append(remark)

        if has_company and company is not None:
            sets.append('company=?'); params.append(company)
        if has_process_id and process_id is not None:
            sets.append('process_id=?'); params.append(int(process_id) if process_id != '' else None)

        # refund：名称查出来后安全截断到 4
        if has_refund and refund_method_id is not None:
            try:
                cur.execute("SELECT name FROM RefundMethod WHERE id=?", int(refund_method_id))
                rr = cur.fetchone()
                refund_name = rr[0] if rr else None
            except Exception:
                refund_name = None
            sets.append('refund=?'); params.append((refund_name or '')[:4])

        # materials
        if material_codes is not None:
            csv_materials = ','.join(str(x).strip() for x in (material_codes or []) if str(x).strip())
            if has_materials:
                sets.append('materials=?'); params.append(csv_materials)
            elif has_material_code:
                first_code = (csv_materials.split(',')[0] if csv_materials else None)
                sets.append('material_code=?'); params.append(first_code)

        # 新字段
        if has_adv and advance_amount is not None:
            sets.append('advance_amount=?'); params.append(_safe_float(advance_amount))
        if has_bal and balance_unpaid is not None:
            sets.append('balance_unpaid=?'); params.append(_safe_float(balance_unpaid))
        if has_inv and is_invoiced is not None:
            sets.append('is_invoiced=?'); params.append(_to_bit(is_invoiced))

        if not sets:
            return {'success': True, 'msg': '无可更新字段'}

        sql = f"UPDATE Mold SET {', '.join(sets)} WHERE id=?"
        params.append(int(mold_id))
        cur.execute(sql, *params)
        conn.commit()
    return {'success': True}

# ====================== 台账：列表（v2） ======================
def list_mold_period_v2():
    with get_dst_connection() as conn:
        cur = conn.cursor()
        # 列探测
        has_materials = _col_exists(cur, 'Mold', 'materials')
        has_material_code = _col_exists(cur, 'Mold', 'material_code')
        has_adv = _col_exists(cur, 'Mold', 'advance_amount')
        has_bal = _col_exists(cur, 'Mold', 'balance_unpaid')
        has_inv = _col_exists(cur, 'Mold', 'is_invoiced')
        has_process = _col_exists(cur, 'Mold', 'process')
        has_company = _col_exists(cur, 'Mold', 'company')
        has_refund = _col_exists(cur, 'Mold', 'refund')

        cols = [
            'm.id','m.product_name',
            's1.supplier_name as casting_supplier',
            's2.supplier_name as mold_supplier',
            'm.start_date','m.end_date','m.amount','m.remark'
        ]
        # 把 materials（或 material_code）放在第 3 列
        if has_materials:
            cols.insert(2, 'm.materials')
        elif has_material_code:
            cols.insert(2, 'm.material_code')

        if has_adv: cols.append('m.advance_amount')
        if has_bal: cols.append('m.balance_unpaid')
        if has_inv: cols.append('m.is_invoiced')
        if has_process: cols.append('m.process')
        if has_company: cols.append('m.company')
        if has_refund: cols.append('m.refund')

        sql = f"""
            SELECT {', '.join(cols)}
            FROM Mold m
            LEFT JOIN Supplier s1 ON m.casting_supplier_id = s1.id
            LEFT JOIN Supplier s2 ON m.mold_supplier_id = s2.id
            ORDER BY m.id DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()

    data = []
    for r in rows:
        idx = 0
        d = {}
        d['mold_id'] = r[idx]; idx += 1
        d['product_name'] = r[idx]; idx += 1

        # materials / material_code 统一返回 'materials'（字符串）
        mat_val = r[idx]; idx += 1
        d['materials'] = mat_val or ''

        d['casting_supplier'] = r[idx]; idx += 1
        d['mold_supplier'] = r[idx]; idx += 1
        d['start_date'] = r[idx]; idx += 1
        d['end_date'] = r[idx]; idx += 1
        d['amount'] = r[idx]; idx += 1
        d['remark'] = r[idx]; idx += 1

        if has_adv: d['advance_amount'] = r[idx]; idx += 1
        if has_bal: d['balance_unpaid'] = r[idx]; idx += 1
        if has_inv: d['is_invoiced'] = r[idx]; idx += 1
        if has_process: d['process'] = r[idx]; idx += 1
        if has_company: d['company'] = r[idx]; idx += 1
        if has_refund: d['refund'] = r[idx]; idx += 1

        data.append(d)
    return data

# ====================== 台账：详情（v2） ======================
def get_mold_period_v2(mold_id: int):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        has_materials = _col_exists(cur, 'Mold', 'materials')
        has_material_code = _col_exists(cur, 'Mold', 'material_code')
        has_adv = _col_exists(cur, 'Mold', 'advance_amount')
        has_bal = _col_exists(cur, 'Mold', 'balance_unpaid')
        has_inv = _col_exists(cur, 'Mold', 'is_invoiced')
        has_process_id = _col_exists(cur, 'Mold', 'process_id')
        has_company = _col_exists(cur, 'Mold', 'company')
        has_refund = _col_exists(cur, 'Mold', 'refund')

        cols = [
            'm.id','m.product_name',
            'm.casting_supplier_id','m.mold_supplier_id',
            'm.start_date','m.end_date','m.amount','m.remark'
        ]
        # 第 3 列放 materials（或 material_code）
        if has_materials:
            cols.insert(2, 'm.materials')
        elif has_material_code:
            cols.insert(2, 'm.material_code')

        if has_adv: cols.append('m.advance_amount')
        if has_bal: cols.append('m.balance_unpaid')
        if has_inv: cols.append('m.is_invoiced')
        if has_process_id: cols.append('m.process_id')
        if has_company: cols.append('m.company')
        if has_refund: cols.append('m.refund')

        sql = f"""
            SELECT {', '.join(cols)},
                   s1.supplier_name as casting_supplier, s2.supplier_name as mold_supplier
            FROM Mold m
            LEFT JOIN Supplier s1 ON m.casting_supplier_id = s1.id
            LEFT JOIN Supplier s2 ON m.mold_supplier_id = s2.id
            WHERE m.id=?
        """
        cur.execute(sql, mold_id)
        row = cur.fetchone()

        # 附件列表
        try:
            cur.execute("SELECT id, file_name FROM MoldAttachment WHERE mold_id=? ORDER BY id", mold_id)
            atts = [{'id': a[0], 'file_name': a[1]} for a in cur.fetchall()]
        except Exception:
            atts = []
    if not row:
        return None

    idx = 0
    d = {}
    d['mold_id'] = row[idx]; idx += 1
    d['product_name'] = row[idx]; idx += 1

    mat_val = row[idx]; idx += 1
    d['materials'] = mat_val or ''

    d['casting_supplier_id'] = row[idx]; idx += 1
    d['mold_supplier_id'] = row[idx]; idx += 1
    d['start_date'] = row[idx]; idx += 1
    d['end_date'] = row[idx]; idx += 1
    d['amount'] = row[idx]; idx += 1
    d['remark'] = row[idx]; idx += 1

    if has_adv: d['advance_amount'] = row[idx]; idx += 1
    if has_bal: d['balance_unpaid'] = row[idx]; idx += 1
    if has_inv: d['is_invoiced'] = row[idx]; idx += 1
    if has_process_id: d['process_id'] = row[idx]; idx += 1
    if has_company: d['company'] = row[idx]; idx += 1
    if has_refund: d['refund'] = row[idx]; idx += 1

    d['casting_supplier'] = row[idx]; idx += 1
    d['mold_supplier'] = row[idx]; idx += 1
    d['attachments'] = atts
    return d

# ====================== 旧接口的兼容包装 ======================
def add_mold_period(product_name, material_code, casting_supplier_id, mold_supplier_id,
                    amount, refund, start_date, end_date, remark, attachments=None):
    """老版本：仅保存单个 material_code / refund 文本。保持兼容。"""
    material_codes = [material_code] if material_code else None
    return add_mold_period_v2(
        product_name=product_name,
        casting_supplier_id=casting_supplier_id,
        mold_supplier_id=mold_supplier_id,
        amount=amount,
        refund_method_id=None,  # 老版本若直接传文字 refund，可在 update_v2 里通过 refund_method_id 统一处理
        process_id=None,
        company=None,
        start_date=start_date,
        end_date=end_date,
        remark=remark,
        material_codes=material_codes,
        attachments=attachments
    )

def list_mold_period():
    return list_mold_period_v2()

def get_mold_period(mold_id):
    return get_mold_period_v2(mold_id)

# ====================== 删除台账 ======================
def delete_mold_period(mold_id: int):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        # 先删附件记录与文件
        try:
            cur.execute("SELECT id, file_path FROM MoldAttachment WHERE mold_id=?", mold_id)
            rows = cur.fetchall()
            for a_id, p in rows:
                if p and os.path.isfile(p):
                    try:
                        os.remove(p)
                    except Exception:
                        pass
            cur.execute("DELETE FROM MoldAttachment WHERE mold_id=?", mold_id)
        except Exception:
            pass
        # 再删主表
        cur.execute("DELETE FROM Mold WHERE id=?", mold_id)
        conn.commit()
    # 删除目录（若空）
    mold_dir = os.path.join(UPLOAD_ROOT, str(mold_id))
    if os.path.isdir(mold_dir):
        try:
            os.rmdir(mold_dir)
        except Exception:
            pass
    return {'success': True}

# ====================== 调拨记录（简化） ======================
def list_transfer_records(mold_id: int):
    with get_dst_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT id, mold_id, from_supplier_id, to_supplier_id, transfer_date, note
                FROM MoldTransferRecord
                WHERE mold_id=?
                ORDER BY id DESC
            """, mold_id)
            rows = cur.fetchall()
            data = []
            for r in rows:
                data.append({
                    'id': r[0], 'mold_id': r[1],
                    'from_supplier_id': r[2], 'to_supplier_id': r[3],
                    'transfer_date': r[4], 'note': r[5]
                })
            return data
        except Exception:
            return []
