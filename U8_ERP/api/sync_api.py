from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import traceback
from modules import sync

sync_api = Blueprint('sync_api', __name__)

def ok(data=None, msg='success'):
    return jsonify({'code': 0, 'msg': msg, 'data': data})

def fail(msg, code=-1):
    return jsonify({'code': code, 'msg': msg, 'data': None})

DATE_FMT = "%Y-%m-%d"

def get_dates():
    """
    智能获取区间，若前端未选，默认用今天到今天+30天
    """
    start_date = request.json.get('start_date') if request.method == 'POST' else request.args.get('start_date')
    end_date = request.json.get('end_date') if request.method == 'POST' else request.args.get('end_date')
    try:
        if not start_date or not end_date:
            today = datetime.now().strftime(DATE_FMT)
            end = (datetime.now() + timedelta(days=30)).strftime(DATE_FMT)
            return today, end
        datetime.strptime(start_date, DATE_FMT)
        datetime.strptime(end_date, DATE_FMT)
        return start_date, end_date
    except Exception:
        raise ValueError(f"日期格式应为YYYY-MM-DD, 当前: {start_date}, {end_date}")

@sync_api.route('/inventory', methods=['POST'])
def sync_inventory_api():
    """
    存货档案同步接口（差异同步，不用区间参数）
    """
    try:
        sync.sync_inventory()
        return ok(msg=f'物料同步完成')
    except Exception as e:
        return fail(str(e) + "\n" + traceback.format_exc())

@sync_api.route('/supplier', methods=['POST'])
def sync_supplier_api():
    """
    供应商同步接口（差异同步，不用区间参数）
    """
    try:
        sync.sync_supplier()
        return ok(msg=f'供应商同步完成')
    except Exception as e:
        return fail(str(e) + "\n" + traceback.format_exc())

@sync_api.route('/bom', methods=['POST'])
def sync_bom_api():
    """
    BOM同步接口（差异同步，不用区间参数）
    """
    try:
        sync.sync_bom()
        return ok(msg=f'BOM同步完成')
    except Exception as e:
        return fail(str(e) + "\n" + traceback.format_exc())

@sync_api.route('/mom_order', methods=['POST'])
def sync_mom_order_api():
    """
    生产订单同步接口（全量同步，需区间参数）
    """
    try:
        start_date, end_date = get_dates()
        sync.sync_mom_order(start_date, end_date)
        return ok(msg=f'生产订单同步完成 {start_date} ~ {end_date}')
    except Exception as e:
        return fail(str(e) + "\n" + traceback.format_exc())

@sync_api.route('/prospect_stock', methods=['POST'])
def sync_prospect_stock_api():
    """
    库存展望同步接口（全量同步，需区间参数）
    """
    try:
        start_date, end_date = get_dates()
        sync.sync_prospect_stock(start_date, end_date)
        return ok(msg=f'库存展望同步完成 {start_date} ~ {end_date}')
    except Exception as e:
        return fail(str(e) + "\n" + traceback.format_exc())

@sync_api.route('/all', methods=['POST'])
def sync_all_api():
    """
    一键全量同步接口（需区间参数）
    """
    try:
        start_date, end_date = get_dates()
        sync.sync_all(start_date, end_date)
        return ok(msg=f'全量同步完成 {start_date} ~ {end_date}')
    except Exception as e:
        return fail(str(e) + "\n" + traceback.format_exc())
