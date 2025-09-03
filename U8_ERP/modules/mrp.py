# modules/mrp.py
"""
MRP计算主流程（数据库纯流版）
- 只用数据库表，不用Excel
- 生产订单取自 mom_order
- BOM取自 BOM
- 结果写入 MRPYSJG
- 所有连接由 db/session.py 管理
"""

import pandas as pd
from datetime import datetime
from db.session import get_dst_connection

def fetch_orders(start_date=None, end_date=None):
    """
    从 mom_order 表读取生产订单（可选过滤计划完工日）
    """
    sql = """
        SELECT id, MoCode, InvCode, InvName, DueDate, Qty
        FROM mom_order
        WHERE 1=1
    """
    params = []
    if start_date:
        sql += " AND DueDate >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND DueDate <= ?"
        params.append(end_date)
    with get_dst_connection() as conn:
        df = pd.read_sql(sql, conn, params=params)
    return df

def fetch_bom():
    """
    从 BOM 表获取BOM明细
    """
    sql = """
        SELECT mother_code, child_code, base_qty_n
        FROM BOM
    """
    with get_dst_connection() as conn:
        df = pd.read_sql(sql, conn)
    return df

def fetch_inventory_snapshot():
    """
    取最新的库存快照表数据
    - 只取当天 snapshot_date 的数据
    """
    today = datetime.now().date()
    sql = """
        SELECT cInvCode, qty
        FROM prospect_stock
        WHERE snapshot_date = ?
        AND source_type = '现存量结存数'
    """
    with get_dst_connection() as conn:
        df = pd.read_sql(sql, conn, params=[today])
    return df

def fetch_inventory_name_dict():
    """
    取 Inventory 表的物料名称字典（用于MRPYSJG写入冗余名）
    """
    sql = "SELECT cInvCode, cInvName FROM Inventory"
    with get_dst_connection() as conn:
        df = pd.read_sql(sql, conn)
    return dict(zip(df.cInvCode, df.cInvName))

def clear_today_mrp_result():
    """
    写入MRP结果前，删除今天已有的MRPYSJG数据（防重复/唯一索引冲突）
    """
    today = datetime.now().date()
    sql = "DELETE FROM MRPYSJG WHERE dRequirDate = ?"
    with get_dst_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql, (today,))
        conn.commit()

def calculate_bom_demand(bom_df, result_list, order_row, product_code, quantity):
    """
    递归分解BOM需求
    - bom_df: BOM明细DataFrame
    - result_list: 累加需求结果（dict列表）
    - order_row: 生产订单（Pandas.Series）
    - product_code: 当前分解物料编码
    - quantity: 本次需求数量
    """
    children = bom_df[bom_df['mother_code'] == product_code]
    for _, row in children.iterrows():
        child_code = row['child_code']
        base_qty = row['base_qty_n'] or 0
        demand = float(base_qty) * float(quantity)
        result_list.append({
            'cinvcode': child_code,
            'Total_demand': demand,
            'dRequirDate': order_row['DueDate']
        })
        # 递归处理多级BOM
        calculate_bom_demand(bom_df, result_list, order_row, child_code, demand)

def run_mrp(start_date=None, end_date=None):
    """
    主流程：1.读订单 2.BOM分解 3.取库存 4.合并需求 5.写入MRPYSJG
    """
    print("🔍 正在读取生产订单...")
    orders_df = fetch_orders(start_date, end_date)
    print(f"共{len(orders_df)}条生产订单。")
    print("🔍 读取BOM明细...")
    bom_df = fetch_bom()
    print(f"BOM共{len(bom_df)}条记录。")
    print("🔍 读取库存快照...")
    inventory_df = fetch_inventory_snapshot()
    inventory_map = dict(zip(inventory_df.cInvCode, inventory_df.qty))
    name_dict = fetch_inventory_name_dict()

    # MRP需求结果临时表（key: cinvcode+dRequirDate, value: 总需求）
    mrp_result = {}

    print("🔄 进行MRP递归分解...")
    for _, order_row in orders_df.iterrows():
        # 对每条订单，先累加本级需求
        key = (order_row['InvCode'], order_row['DueDate'])
        mrp_result[key] = mrp_result.get(key, 0) + float(order_row['Qty'])
        # 再递归分解BOM
        result_list = []
        calculate_bom_demand(bom_df, result_list, order_row, order_row['InvCode'], order_row['Qty'])
        for r in result_list:
            k = (r['cinvcode'], r['dRequirDate'])
            mrp_result[k] = mrp_result.get(k, 0) + float(r['Total_demand'])

    # 开始写入数据库
    clear_today_mrp_result()
    print("📝 写入MRPYSJG表...")
    today = datetime.now().date()
    with get_dst_connection() as conn:
        cursor = conn.cursor()
        count = 0
        for (cinvcode, dRequirDate), total_demand in mrp_result.items():
            # 冗余物料名
            cinvname = name_dict.get(cinvcode, "")
            # 获取库存
            as_quantity = inventory_map.get(cinvcode, 0)
            # 若有多地库存、需合并/区分，可自行扩展
            cursor.execute("""
                INSERT INTO MRPYSJG
                (cinvcode, cinvname, Total_demand, dRequirDate, AS_iQuantity)
                VALUES (?, ?, ?, ?, ?)
            """, cinvcode, cinvname, total_demand, dRequirDate, as_quantity)
            count += 1
        conn.commit()
    print(f"✅ 共写入MRP明细{count}条。")

if __name__ == '__main__':
    # 可传递参数限制计算范围
    run_mrp()
