# modules/sync.py
import os
from datetime import datetime, date
from db.session import get_u8_connection, get_dst_connection

# ---- 类型安全转换工具 ----
def safe_str(x, maxlen=None):
    if x is None:
        return ""
    s = str(x).strip()
    if maxlen is not None and len(s) > maxlen:
        return s[:maxlen]
    return s

def safe_float(x):
    try: return float(x) if x is not None else 0.0
    except Exception: return 0.0

def safe_int(x):
    try: return int(x) if x is not None else 0
    except Exception: return 0

def safe_date(x):
    if x is None: return datetime.now().date()
    if isinstance(x, str):
        try: return datetime.strptime(x, "%Y-%m-%d").date()
        except: return datetime.now().date()
    if isinstance(x, datetime): return x.date()
    if isinstance(x, date): return x
    return datetime.now().date()

def safe_datetime(x):
    from datetime import datetime
    if x is None:
        return datetime.now()
    if isinstance(x, datetime):
        return x
    if isinstance(x, str):
        try:
            return datetime.strptime(x[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                return datetime.strptime(x[:10], "%Y-%m-%d")
            except Exception:
                return datetime.now()
    return datetime.now()


# 1. 存货档案同步
def sync_inventory(error_list=None):
    """存货档案表差异同步"""
    with get_u8_connection() as u8, get_dst_connection() as dst:
        u8_cur, dst_cur = u8.cursor(), dst.cursor()
        u8_cur.execute("SELECT cInvCode, cInvName FROM inventory")
        u8_data = {safe_str(r[0]): safe_str(r[1]) for r in u8_cur.fetchall()}
        dst_cur.execute("SELECT cInvCode, cInvName FROM Inventory")
        dst_data = {safe_str(r[0]): safe_str(r[1]) for r in dst_cur.fetchall()}
        # 新增/更新
        for code, name in u8_data.items():
            if code in dst_data:
                if name != dst_data[code]:
                    try:
                        dst_cur.execute("UPDATE Inventory SET cInvName=? WHERE cInvCode=?", (name, code))
                    except Exception as ex:
                        if error_list is not None: error_list.append(f"[Inventory-UPDATE]{code}-{name} {ex}")
            else:
                try:
                    dst_cur.execute("INSERT INTO Inventory (cInvCode, cInvName) VALUES (?,?)", (code, name))
                except Exception as ex:
                    if error_list is not None: error_list.append(f"[Inventory-INSERT]{code}-{name} {ex}")
        # 删除
        to_delete = set(dst_data.keys()) - set(u8_data.keys())
        for code in to_delete:
            try:
                dst_cur.execute("DELETE FROM Inventory WHERE cInvCode=?", (code,))
            except Exception as ex:
                if error_list is not None: error_list.append(f"[Inventory-DELETE]{code} {ex}")
        dst.commit()
    print('Inventory差异同步完成')

# 2. 供应商同步
def sync_supplier(error_list=None):
    """供应商档案同步"""
    with get_u8_connection() as u8, get_dst_connection() as dst:
        u8_cur, dst_cur = u8.cursor(), dst.cursor()
        u8_cur.execute("SELECT cVenName, cVenPerson, cVenPhone FROM Vendor")
        u8_data = {safe_str(r[0]): (safe_str(r[1]), safe_str(r[2])) for r in u8_cur.fetchall()}
        dst_cur.execute("SELECT supplier_name, contact, phone FROM Supplier")
        dst_data = {safe_str(r[0]): (safe_str(r[1]), safe_str(r[2])) for r in dst_cur.fetchall()}
        for name, (person, phone) in u8_data.items():
            if name in dst_data:
                if (person, phone) != dst_data[name]:
                    try:
                        dst_cur.execute("UPDATE Supplier SET contact=?, phone=? WHERE supplier_name=?", (person, phone, name))
                    except Exception as ex:
                        if error_list is not None: error_list.append(f"[Supplier-UPDATE]{name} {ex}")
            else:
                try:
                    dst_cur.execute("INSERT INTO Supplier (supplier_name, contact, phone) VALUES (?, ?, ?)", (name, person, phone))
                except Exception as ex:
                    if error_list is not None: error_list.append(f"[Supplier-INSERT]{name} {ex}")
        # 删除
        to_delete = set(dst_data.keys()) - set(u8_data.keys())
        for name in to_delete:
            try:
                dst_cur.execute("DELETE FROM Supplier WHERE supplier_name=?", (name,))
            except Exception as ex:
                if error_list is not None: error_list.append(f"[Supplier-DELETE]{name} {ex}")
        dst.commit()
    print('Supplier差异同步完成')

# 3. BOM同步
def sync_bom(error_list=None):
    """BOM差异同步（主键：母件编码+版本+子件编码+工序）"""
    BATCH_SIZE = 1000
    with get_u8_connection() as u8, get_dst_connection() as dst:
        u8_cur, dst_cur = u8.cursor(), dst.cursor()
        # 查询U8 BOM所有数据
        u8_cur.execute("""
            SELECT 
                E.cInvCode, E.cInvName, E.cInvStd, F.cComUnitName, C.ParentScrap,
                A.Version, A.VersionDesc, A.VersionEffDate, A.IdentCode, A.IdentDesc,
                CASE WHEN A.CloseTime IS NULL THEN '审核' ELSE '停用' END,
                CASE WHEN E.iPlanDefault = 1 THEN '自制件'
                     WHEN E.iPlanDefault = 2 THEN '委外件'
                     WHEN E.iPlanDefault = 3 THEN '采购件' END,
                A.ApplyDId, NULL, B.SortSeq, B.OpSeq, NULL,
                H.cInvCode, H.cInvName, H.cInvStd, I.cComUnitName, B.BaseQtyN, B.BaseQtyD, B.CompScrap,
                CASE WHEN B.FVFlag = 0 THEN '是' ELSE '否' END, 
                CASE WHEN H.iSupplyType = 0 THEN '领用'
                     WHEN H.iSupplyType = 1 THEN '入库倒冲'
                     WHEN H.iSupplyType = 2 THEN '工序倒冲'
                     WHEN H.iSupplyType = 3 THEN '虚拟件' END,
                B.BaseQtyN, B.EffBegDate, B.EffEndDate,
                CASE WHEN B.ByproductFlag = 0 THEN '否' END, 
                CASE WHEN H.iPlanDefault = 1 THEN '自制件'
                     WHEN H.iPlanDefault = 2 THEN '委外件'
                     WHEN H.iPlanDefault = 3 THEN '采购件' END,
                B.Remark
            FROM bom_bom A
            LEFT JOIN bom_opcomponent B ON A.BomId = B.BomId
            LEFT JOIN bom_parent C ON A.BomId = C.BomId
            LEFT JOIN bas_part D ON C.ParentId = D.PartId
            LEFT JOIN Inventory E ON D.InvCode = E.cInvCode
            LEFT JOIN ComputationUnit F ON F.cComunitCode = E.cComUnitCode
            LEFT JOIN bas_part G ON B.ComponentId = G.PartId
            LEFT JOIN Inventory H ON G.InvCode = H.cInvCode
            LEFT JOIN ComputationUnit I ON H.cComunitCode = I.cComUnitCode
            WHERE A.CloseTime IS NULL and E.cInvCode is not null
        """)
        u8_rows = u8_cur.fetchall()
        print(f"[INFO] 从U8读取到{len(u8_rows)}条BOM数据")
        # 获取本地所有BOM主键集合
        dst_cur.execute("SELECT mother_code, version, child_code, process_seq FROM BOM")
        local_keys = set((r[0], r[1], r[2], r[3]) for r in dst_cur.fetchall())
        u8_keys = set()
        # 批量插入准备
        insert_rows = []
        for idx, row in enumerate(u8_rows):
            try:
                row_safe = tuple(
                    safe_str(x) if isinstance(x, str) or x is None
                    else safe_float(x) if isinstance(x, float)
                    else safe_int(x) if isinstance(x, int)
                    else safe_date(x) if isinstance(x, (datetime, date))
                    else x
                    for x in row
                )
                # 主键：母件编码、版本、子件编码、工序
                k = (row_safe[0], row_safe[5], row_safe[17], row_safe[15])
                u8_keys.add(k)
                if k in local_keys:
                    continue  # 已存在，无需插入
                insert_rows.append(row_safe)
            except Exception as ex:
                if error_list is not None: error_list.append(f"[BOM-ROW][{idx}]{ex}")
        # 批量写入
        insert_sql = """
            INSERT INTO BOM (
                mother_code, mother_name, mother_std, mother_unit, parent_scrap,
                version, version_desc, version_effdate, ident_code, ident_desc, status,
                mother_type, apply_did, row_no, child_sort_seq, process_seq, process_name,
                child_code, child_name, child_std, child_unit, base_qty_n, base_qty_d, comp_scrap,
                is_fixed, supply_type, use_qty, eff_beg_date, eff_end_date, is_byproduct,
                material_type, remark
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        total_inserted = 0
        for i in range(0, len(insert_rows), BATCH_SIZE):
            batch = insert_rows[i:i + BATCH_SIZE]
            try:
                dst_cur.executemany(insert_sql, batch)
                total_inserted += len(batch)
            except Exception as ex:
                if error_list is not None: error_list.append(f"[BOM-BATCH-INSERT][{i}]{ex}")
        # 删除本地多余BOM
        to_delete = local_keys - u8_keys
        delete_sql = "DELETE FROM BOM WHERE mother_code=? AND version=? AND child_code=? AND process_seq=?"
        delete_rows = list(to_delete)
        for i in range(0, len(delete_rows), BATCH_SIZE):
            batch = delete_rows[i:i + BATCH_SIZE]
            try:
                dst_cur.executemany(delete_sql, batch)
            except Exception as ex:
                if error_list is not None: error_list.append(f"[BOM-BATCH-DELETE][{i}]{ex}")
        dst.commit()
        print(f"[INFO] BOM同步完成：新增{total_inserted}条，删除{len(to_delete)}条")
    print('BOM差异同步完成')

# 4. 生产订单同步
def sync_mom_order(start_date, end_date, error_list=None):
    """生产订单全量同步"""
    BATCH_SIZE = 1000
    # 1. 先清空本地 mom_order 表
    with get_dst_connection() as dst:
        dst_cur = dst.cursor()
        dst_cur.execute("TRUNCATE TABLE mom_order")
        dst.commit()
    # 2. 拉取U8区间生产订单数据
    sql = """
        SELECT A.MoCode, B.sortseq, G.EnumName, H.EnumName, I.EnumName, B.InvCode, 
               E.cInvName, C.StartDate, C.DueDate, F.cComUnitName, B.Qty, B.MrpQty, B.MDeptCode,
               D.cDepName, B.DeclaredQty, B.QualifiedInQty, (B.Qty - B.QualifiedInQty), B.Define31,
               B.Define33, J.EnumName, B.DemandCode, A.CreateUser, B.CloseUser, A.Define11
        FROM mom_order A
        LEFT JOIN mom_orderdetail B ON A.MoId = B.MoId
        LEFT JOIN mom_morder C ON A.MoId = C.MoId
        LEFT JOIN Department D ON B.MDeptCode = D.cDepCode
        LEFT JOIN Inventory E ON B.InvCode = E.cInvCode
        LEFT JOIN ComputationUnit F ON E.cComUnitCode = F.cComunitCode
        LEFT JOIN (select * from AA_Enum where enumtype = 'MO.Status' AND LocaleID = 'zh-CN') G ON B.Status = G.EnumCode
        LEFT JOIN (select * from AA_Enum where enumtype = 'MO.AuditStatus' AND LocaleID = 'zh-CN') H ON B.AuditStatus = H.EnumCode
        LEFT JOIN (select * from AA_Enum where enumtype = 'MO.MoClass' AND LocaleID = 'zh-CN') I ON B.MoClass = I.EnumCode
        LEFT JOIN (select * from AA_Enum where enumtype = 'MO.SoType' AND LocaleID = 'zh-CN') J ON B.SoType = J.EnumCode
        WHERE G.EnumName = '审核'
          AND C.DueDate >= ? AND C.DueDate <= ?
    """
    batch_rows = []
    with get_u8_connection() as u8, get_dst_connection() as dst:
        u8_cur, dst_cur = u8.cursor(), dst.cursor()
        u8_cur.execute(sql, (start_date, end_date))
        all_rows = u8_cur.fetchall()
        print(f"[INFO] 查询U8生产订单 {len(all_rows)} 条")
        for idx, row in enumerate(all_rows):
            try:
                row_safe = tuple(
                    safe_str(x) if isinstance(x, str) or x is None
                    else safe_float(x) if isinstance(x, float)
                    else safe_int(x) if isinstance(x, int)
                    else safe_date(x) if isinstance(x, (datetime, date))
                    else x
                    for x in row
                )
                batch_rows.append(row_safe)
            except Exception as ex:
                if error_list is not None: error_list.append(f"[mom_order-ROW][{idx}]{ex}")
        insert_sql = """
            INSERT INTO mom_order (
                MoCode, sortseq, status, audit_status, mo_type, InvCode,
                InvName, StartDate, DueDate, UnitName, Qty, MrpQty, MDeptCode,
                DepName, DeclaredQty, QualifiedInQty, UnfinishedQty, Assembler,
                SOCode, track_type, DemandCode, CreateUser, CloseUser, Define11
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        total_inserted = 0
        for i in range(0, len(batch_rows), BATCH_SIZE):
            batch = batch_rows[i:i + BATCH_SIZE]
            try:
                dst_cur.executemany(insert_sql, batch)
                total_inserted += len(batch)
            except Exception as ex:
                if error_list is not None: error_list.append(f"[mom_order-BATCH-INSERT][{i}]{ex}")
        dst.commit()
        print(f"[INFO] mom_order同步完成，共插入{total_inserted}条")
    print('mom_order全量同步完成')

# 5. 库存展望全量同步
def sync_prospect_stock(start_date, end_date, error_list=None):
    """库存展望表全量同步（目标库连接）"""

    # 定义每个字段最大长度，便于 safe_str 截断
    CINVCODE_MAXLEN = 50
    CINVNAME_MAXLEN = 100
    SOURCETYPE_MAXLEN = 50

    # 1. 先清空本地表
    with get_dst_connection() as dst_conn:
        dst_cur = dst_conn.cursor()
        dst_cur.execute("TRUNCATE TABLE prospect_stock")
        dst_conn.commit()

    # 日期参数处理
    if isinstance(end_date, (datetime, date)):
        end_date_str = end_date.strftime('%Y-%m-%d')
    else:
        end_date_str = str(end_date)
    snapshot_date = datetime.now().date()
    created_time = datetime.now()

    sqls = [
        # 1. 现存量结存数
        ("""
            SELECT a.cInvCode, b.cInvName, SUM(a.iQuantity) AS qty, '现存量结存数' AS source_type
            FROM CurrentStock a
            LEFT JOIN inventory b ON a.cInvCode = b.cInvCode
            WHERE a.iQuantity > 0 AND a.cwhcode IN ('0401','0402','0403') AND a.cInvCode NOT LIKE '51%'
            GROUP BY a.cInvCode, b.cInvName
        """),
        # 2. 在途采购订单数
        (f"""
            SELECT a.cInvCode, c.cInvName, SUM(a.iQuantity - ISNULL(e.在途采购订单数, 0)) AS qty, '在途采购订单数' AS source_type
            FROM PO_Podetails a
            LEFT JOIN PO_Pomain b ON a.POID = b.POID
            LEFT JOIN Inventory c ON a.cInvCode = c.cInvCode
            LEFT JOIN (
                SELECT cInvCode, iPOsID, SUM(iQuantity) AS 在途采购订单数
                FROM rdrecords01
                GROUP BY cInvCode, iPOsID
            ) e ON a.ID = e.iPOsID
            WHERE a.dArriveDate <= '{end_date_str}' AND a.cbCloser IS NULL AND b.cPOID IS NOT NULL AND a.cInvCode NOT LIKE '51%'
            GROUP BY a.cInvCode, c.cInvName
        """),
        # 3. 采购到货待检数
        (f"""
            SELECT d.cInvCode, a.cInvName, SUM(d.iQuantity) AS qty, '采购到货待检数' AS source_type
            FROM rdrecords01 d
            LEFT JOIN RdRecord01 e ON d.ID = e.ID
            LEFT JOIN inventory a ON a.cInvCode = d.cInvCode
            WHERE e.dDate <= '{end_date_str}' AND e.cHandler IS NULL AND d.cInvCode NOT LIKE '51%'
            GROUP BY d.cInvCode, a.cInvName
        """),
        # 4. 生产未完成数量
        (f"""
            SELECT B.InvCode, E.cInvName, SUM(B.Qty - B.QualifiedInQty) AS qty, '生产未完成数量' AS source_type
            FROM mom_order A
            LEFT JOIN mom_orderdetail B ON A.MoId = B.MoId
            LEFT JOIN mom_morder C ON A.MoId = C.MoId
            LEFT JOIN Inventory E ON B.InvCode = E.cInvCode
            WHERE C.DueDate <= '{end_date_str}' AND B.Status <> '4' AND B.InvCode NOT LIKE '51%'
            GROUP BY B.InvCode, E.cInvName
        """),
        # 5. 销售订单未发货数量
        (f"""
            SELECT a.cInvCode, b.cInvName, SUM(a.iQuantity - ISNULL(d.发货数量, 0)) AS qty, '销售订单未发货数量' AS source_type
            FROM SO_SODetails a
            LEFT JOIN Inventory b ON a.cInvCode = b.cInvCode
            LEFT JOIN SO_SOMain c ON a.cSOCode = c.cSOCode
            LEFT JOIN (
                SELECT cInvCode, iSOsID, SUM(iQuantity) AS 发货数量
                FROM DispatchLists
                GROUP BY cInvCode, iSOsID
            ) d ON a.iSOsID = d.iSOsID
            WHERE c.dPreDateBT <= '{end_date_str}' AND a.cSCloser IS NULL AND c.cVerifier IS NOT NULL AND a.cInvCode NOT LIKE '51%'
            GROUP BY a.cInvCode, b.cInvName
        """),
        # 6. 发货未出库数量
        (f"""
            SELECT a.cInvCode, c.cInvName, SUM(a.iQuantity - ISNULL(d.出库数量, 0)) AS qty, '发货未出库数量' AS source_type
            FROM DispatchLists a
            LEFT JOIN DispatchList b ON a.DLID = b.DLID
            LEFT JOIN Inventory c ON a.cInvCode = c.cInvCode
            LEFT JOIN (
                SELECT cInvCode, iDLsID, SUM(iQuantity) AS 出库数量
                FROM rdrecords32
                GROUP BY cInvCode, iDLsID
            ) d ON a.iDLsID = d.iDLsID
            WHERE b.dDate <= '{end_date_str}' AND a.cSCloser IS NULL AND a.cInvCode NOT LIKE '51%'
            GROUP BY a.cInvCode, c.cInvName
        """),
        # 7. 材料出库单未审核数量
        (f"""
            SELECT b.cInvCode, c.cInvName, b.iQuantity AS qty, '材料出库单未审核数量' AS source_type
            FROM rdrecords11 b
            LEFT JOIN rdRecord11 a ON a.ID = b.ID
            LEFT JOIN Inventory c ON b.cInvCode = c.cInvCode
            WHERE a.dDate <= '{end_date_str}' AND a.cHandler IS NULL AND b.cInvCode NOT LIKE '51%'
        """),
        # 8. 生产未领料数量
        (f"""
            SELECT A.InvCode, D.cInvName, SUM(A.Qty - A.IssQty) AS qty, '生产未领料数量' AS source_type
            FROM mom_moallocate A
            LEFT JOIN mom_orderdetail B ON A.MoDId = B.MoDId
            LEFT JOIN mom_morder C ON B.MoId = C.MoId
            LEFT JOIN Inventory D ON A.InvCode = D.cInvCode
            WHERE C.DueDate <= '{end_date_str}' AND B.Status <> '4' AND A.InvCode NOT LIKE '51%'
            GROUP BY A.InvCode, D.cInvName
        """)
    ]
    BATCH_SIZE = 3000  # 每批插入条数
    buffer = []

    with get_u8_connection() as u8_conn, get_dst_connection() as dst_conn:
        u8_cur, dst_cur = u8_conn.cursor(), dst_conn.cursor()
        for sql in sqls:
            try:
                u8_cur.execute(sql)
                rows = u8_cur.fetchall()
                for row in rows:
                    try:
                        cInvCode = safe_str(row[0], CINVCODE_MAXLEN)
                        cInvName = safe_str(row[1], CINVNAME_MAXLEN)
                        qty = safe_float(row[2])  # DECIMAL(18,4)
                        source_type = safe_str(row[3], SOURCETYPE_MAXLEN)
                        snap_date = safe_date(snapshot_date).strftime('%Y-%m-%d')  # DATE to str
                        create_time = safe_datetime(datetime.now()).strftime('%Y-%m-%d %H:%M:%S')  # DATETIME to str

                        buffer.append((cInvCode, cInvName, qty, source_type, snap_date, create_time))

                        # 满BATCH_SIZE就批量写入并commit
                        if len(buffer) >= BATCH_SIZE:
                            try:
                                dst_cur.executemany(
                                    "INSERT INTO prospect_stock (cInvCode, cInvName, qty, source_type, snapshot_date, created_time) VALUES (?, ?, ?, ?, ?, ?)",
                                    buffer
                                )
                                dst_conn.commit()
                                buffer.clear()
                            except Exception as ex:
                                print("[prospect_stock-BATCH-INSERT]", ex)
                                if error_list is not None:
                                    error_list.append(f"[prospect_stock-BATCH-INSERT] {ex}")
                                buffer.clear()
                    except Exception as ex:
                        print("[prospect_stock-INSERT]", ex)
                        if error_list is not None:
                            error_list.append(f"[prospect_stock-INSERT] {ex}  {row}")
                # 每个SQL执行完，及时插入剩余不足一批的
                if buffer:
                    try:
                        dst_cur.executemany(
                            "INSERT INTO prospect_stock (cInvCode, cInvName, qty, source_type, snapshot_date, created_time) VALUES (?, ?, ?, ?, ?, ?)",
                            buffer
                        )
                        dst_conn.commit()
                    except Exception as ex:
                        print("[prospect_stock-BATCH-INSERT-LAST]", ex)
                        if error_list is not None:
                            error_list.append(f"[prospect_stock-BATCH-INSERT-LAST] {ex}")
                    buffer.clear()
            except Exception as ex:
                print("[prospect_stock-SELECT]", ex)
                if error_list is not None:
                    error_list.append(f"[prospect_stock-SELECT] {ex}")
    print('prospect_stock全量同步完成')


# ========== 主调度入口 ==========
def sync_all(start_date, end_date):
    """
    主调度入口：按前端传递的区间参数调用各同步模块
    """
    error_list = []
    sync_inventory(error_list)
    sync_supplier(error_list)
    sync_bom(error_list)
    sync_mom_order(start_date, end_date, error_list)
    sync_prospect_stock(start_date, end_date, error_list)
    error_file = os.path.abspath("sync_error_log.txt")
    with open(error_file, "w", encoding="utf-8") as f:
        if error_list:
            for i, err in enumerate(error_list, 1):
                f.write(f"【{i}】{err}\n\n")
        else:
            f.write("本次同步无异常。\n")
    print(f"[调试] 日志输出路径：{error_file}")

if __name__ == '__main__':
    sync_all('2025-07-01', '2025-07-31')
