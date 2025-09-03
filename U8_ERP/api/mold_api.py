# api/mold_api.py
from flask import Blueprint, request, jsonify, send_file
from modules import mold
import json
import os
import io, csv, datetime
try:
    import pandas as pd
except Exception:
    pd = None
from flask import Response

mold_api = Blueprint('mold_api', __name__)

# ---------- 下拉 / 搜索 ----------
@mold_api.route('/supplier/search')
def supplier_search():
    kw = request.args.get('kw', '').strip()
    return jsonify(mold.search_supplier(kw))

@mold_api.route('/inventory/search')
def inventory_search():
    kw = request.args.get('kw', '').strip()
    return jsonify(mold.search_inventory(kw))

@mold_api.route('/dictionary/refund_methods')
def dict_refund_methods():
    """返还方式下拉：Mujufanhuan"""
    return jsonify(mold.list_refund_methods())

@mold_api.route('/dictionary/process_methods')
def dict_process_methods():
    """模具工艺下拉：MJGYi"""
    return jsonify(mold.list_process_methods())

# ---------- 工具 ----------
def _parse_material_codes(form):
    """
    兼容三种传法：
    - material_codes=["A","B"]  (JSON)
    - material_codes="A,B,C"    (逗号分隔)
    - material_code="A"         (单个老字段)
    """
    raw = form.get('material_codes', '').strip()
    if raw:
        # 优先按 JSON 解析
        try:
            arr = json.loads(raw)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
        # 退化为逗号分隔
        return [s.strip() for s in raw.split(',') if s.strip()]

    single = form.get('material_code', '').strip()
    return [single] if single else []

def _safe_float(x):
    try:
        return float(x) if x not in (None, '') else None
    except Exception:
        return None

def _safe_int(x):
    try:
        return int(x) if x not in (None, '') else None
    except Exception:
        return None

# ---------- 台账：新增/更新/列表/详情/删除 ----------
@mold_api.route('/period', methods=['POST'])
def add_mold_period():
    """
    对齐 modules.mold.add_mold_period_v2
    需要关键字段：
      product_name, casting_supplier_id, mold_supplier_id,
      refund_method_id, process_id, company,
      amount, start_date, end_date, remark,
      material_codes(见兼容规则)
    """
    form = request.form
    product_name = form.get('product_name', '').strip()
    casting_supplier_id = form.get('casting_supplier_id')
    mold_supplier_id = form.get('mold_supplier_id')
    amount = _safe_float(form.get('amount'))
    refund_method_id = _safe_int(form.get('refund_method_id'))
    process_id = _safe_int(form.get('process_id'))
    company = form.get('company', '').strip() or None
    start_date = form.get('start_date') or None
    end_date = form.get('end_date') or None
    remark = form.get('remark', '')
    material_codes = _parse_material_codes(form)
    attachments = request.files.getlist('attachments')

    result = mold.add_mold_period_v2(
        product_name=product_name,
        casting_supplier_id=casting_supplier_id,
        mold_supplier_id=mold_supplier_id,
        amount=amount,
        refund_method_id=refund_method_id,
        process_id=process_id,
        company=company,
        start_date=start_date,
        end_date=end_date,
        remark=remark,
        material_codes=material_codes,
        attachments=attachments
    )
    return jsonify(result)

@mold_api.route('/period/<int:mold_id>', methods=['PUT'])
def update_mold_period(mold_id):
    """
    对齐 modules.mold.update_mold_period_v2
    传参规则与新增一致（attachments 如需改名/加删，请调附件接口）
    """
    data = request.get_json(silent=True) or {}
    # 同时兼容 x-www-form-urlencoded
    form = request.form if request.form else {}

    def pick(name, default=None):
        return data.get(name, form.get(name, default))

    product_name = pick('product_name')
    casting_supplier_id = pick('casting_supplier_id')
    mold_supplier_id = pick('mold_supplier_id')
    amount = _safe_float(pick('amount'))
    refund_method_id = _safe_int(pick('refund_method_id'))
    process_id = _safe_int(pick('process_id'))
    company = pick('company')
    start_date = pick('start_date')
    end_date = pick('end_date')
    remark = pick('remark')
    # 解析物料清单
    material_codes = []
    if 'material_codes' in data or 'material_code' in data:
        mc = data.get('material_codes')
        if isinstance(mc, list):
            material_codes = [str(x).strip() for x in mc if str(x).strip()]
        elif isinstance(mc, str) and mc.strip():
            material_codes = [s.strip() for s in mc.split(',') if s.strip()]
        single = data.get('material_code', '')
        if single and single not in material_codes:
            material_codes.append(single)
    else:
        material_codes = _parse_material_codes(form)

    result = mold.update_mold_period_v2(
        mold_id=mold_id,
        product_name=product_name,
        casting_supplier_id=casting_supplier_id,
        mold_supplier_id=mold_supplier_id,
        amount=amount,
        refund_method_id=refund_method_id,
        process_id=process_id,
        company=company,
        start_date=start_date,
        end_date=end_date,
        remark=remark,
        material_codes=material_codes
    )
    return jsonify(result)

@mold_api.route('/period/list')
def period_list():
    return jsonify({'data': mold.list_mold_period_v2()})

@mold_api.route('/period/<int:mold_id>')
def period_detail(mold_id):
    try:
        info = mold.get_mold_period_v2(mold_id)
        if not info:
            return jsonify({'success': False, 'msg': '未找到台账'}), 404
        return jsonify(info)
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({'success': False, 'msg': str(e)}), 500

@mold_api.route('/period/delete/<int:mold_id>', methods=['POST'])
def period_delete(mold_id):
    result = mold.delete_mold_period(mold_id)
    return jsonify(result)

# ---------- 附件 ----------
@mold_api.route('/attachment/delete/<int:attachment_id>', methods=['POST'])
def attachment_delete(attachment_id):
    result = mold.delete_attachment(attachment_id)
    return jsonify(result)

@mold_api.route('/attachment/download/<int:attachment_id>')  #------老导出接口，弃用
def attachment_download(attachment_id):
    try:
        files = mold.get_attachments_by_id(attachment_id)
        if not files or not files.get('file_path') or not os.path.isfile(files['file_path']):
            return jsonify({'success': False, 'msg': '文件不存在'}), 404
        return send_file(files['file_path'], as_attachment=True, download_name=files['file_name'])
    except Exception as e:
        import traceback
        print('【ERROR】下载接口traceback：\n', traceback.format_exc())
        return jsonify({'success': False, 'msg': '下载失败: ' + str(e)}), 500

# ---------- 兼容：模具搜索（简单用列表结果做前端下拉/模糊） ----------
@mold_api.route('/mold/search')
def mold_search():
    kw = (request.args.get('kw') or '').strip().lower()
    rows = mold.list_mold_period_v2()
    if kw:
        rows = [r for r in rows if kw in (r.get('product_name','') or '').lower()]
    # 精简返回
    brief = [{'mold_id': r['mold_id'], 'product_name': r['product_name']} for r in rows]
    return jsonify(brief)
@mold_api.route('/period/export/csv', methods=['GET'])
def period_export_csv():
    """
    导出全部模具列表为 CSV（UTF-8-SIG，Excel 直接打开不乱码）
    可选查询参数：
      - fields: 用逗号分隔的字段名，默认导出全部常用列
    """
    rows = mold.list_mold_period_v2()  # 数据来源
    if not rows:
        return jsonify({'success': False, 'msg': '没有可导出的数据'}), 404

    # 默认导出列（与 list_mold_period_v2 返回的键一致）
    default_fields = [
        'mold_id','product_name','casting_supplier','mold_supplier','materials',
        'start_date','end_date','amount','refund','process','company','remark'
    ]
    fields_param = (request.args.get('fields') or '').strip()
    fields = [f.strip() for f in fields_param.split(',') if f.strip()] or default_fields

    # 中文表头（可按需调整）
    header_map = {
        'mold_id':'模具ID','product_name':'产品名称','casting_supplier':'铸造供应商',
        'mold_supplier':'模具供应商','materials':'物料编码(汇总)','start_date':'开模时间',
        'end_date':'交付时间','amount':'金额','refund':'返还方式','process':'模具工艺',
        'company':'所属公司','remark':'备注'
    }
    titles = [header_map.get(f, f) for f in fields]

    # 写入 CSV 文本
    sio = io.StringIO(newline='')
    writer = csv.writer(sio)
    writer.writerow(titles)
    for r in rows:
        writer.writerow([ (r.get(f, '') if r.get(f, '') is not None else '') for f in fields ])

    # 关键：以 UTF-8-SIG 编码返回，Excel 不会乱码
    data = sio.getvalue().encode('utf-8-sig')
    sio.close()

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"mold_list_{ts}.csv"
    return Response(
        data,
        mimetype='text/csv; charset=utf-8',
        headers={
            'Content-Disposition': f"attachment; filename={filename}; filename*=UTF-8''{filename}"
        }
    )

@mold_api.route('/period/export/xlsx', methods=['GET'])
def period_export_xlsx():
    """
    导出全部模具列表为 Excel（.xlsx）
    需要 pandas + openpyxl；若环境无 pandas，请使用上面的 CSV 接口
    """
    if pd is None:
        return jsonify({'success': False, 'msg': '服务器未安装 pandas，请改用 /period/export/csv'}), 500

    rows = mold.list_mold_period_v2()
    if not rows:
        return jsonify({'success': False, 'msg': '没有可导出的数据'}), 404

    df = pd.DataFrame(rows, columns=[
        'mold_id','product_name','casting_supplier','mold_supplier','materials',
        'start_date','end_date','amount','refund','process','company','remark'
    ])

    # 可选：重命名表头为中文
    df = df.rename(columns={
        'mold_id':'模具ID','product_name':'产品名称','casting_supplier':'铸造供应商',
        'mold_supplier':'模具供应商','materials':'物料编码(汇总)','start_date':'开模时间',
        'end_date':'交付时间','amount':'金额','refund':'返还方式','process':'模具工艺',
        'company':'所属公司','remark':'备注'
    })

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='模具列表', index=False)
    bio.seek(0)

    ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"mold_list_{ts}.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )