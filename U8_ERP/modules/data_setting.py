# -*- coding: utf-8 -*-
"""
data_setting.py
数据设置模块 - 业务逻辑/数据访问层

覆盖功能：
1) 用户管理：查询 / 新增 / 更新 / 删除
2) 模具费返还方式（Mujufanhuan）：查询 / 新增 / 更新 / 删除
3) 模具工艺（MJGYi）：查询 / 新增 / 更新 / 删除
4) 模具与物料对照（MJWLDZhao）：条件 + 分页查询
"""

from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
import hashlib
from db.session import get_dst_connection

class DataSettingService:
    """数据设置服务"""

    # ==== 表名与列名候选（按优先级从左到右） ====
    # 若你的用户表就是 users，请把 USER_TABLE 改回 "users"
    USER_TABLE = "SysUser"
    USER_REALNAME_CAND = ["real_name", "display_name", "fullname", "realname"]
    USER_PWD_CAND = ["password", "password_hash"]
    USER_CREATED_AT_CAND = ["created_at", "create_time", "creat_time", "creation_time"]
    USER_ENABLED_CAND = ["enabled", "is_enabled", "status", "is_active"]
    USER_ROLE_CAND = ["role", "user_role"]
    USER_NAME_COL = "username"
    USER_ID_COL = "id"

    REFUND_TABLE = "Mujufanhuan"
    REFUND_NAME_CAND = ["M_fangshi", "name", "fangshi", "Mujufanhuan_name"]
    REFUND_ID_COL = "id"

    PROC_TABLE = "MJGYi"
    PROC_NAME_CAND = ["MJ_gongyi", "name", "M_name", "gy_name", "proc_name", "MJGYi_name"]
    PROC_ID_COL = "id"

    MAP_TABLE = "MJWLDZhao"
    MAP_MJID_CAND = ["MJ_id", "mj_id"]
    MAP_MJNAME_CAND = ["MJ_name", "mj_name"]
    MAP_CINVCODE_CAND = ["cinvcode", "cInvCode"]
    MAP_CINVNAME_CAND = ["cinvname", "cInvName"]

    # ---------- 私有工具 ----------
    def _get_columns(self, cursor, table: str) -> List[str]:
        sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        """
        cursor.execute(sql, (table,))
        return [r[0] for r in cursor.fetchall()]

    def _pick_col(self, columns: List[str], candidates: List[str], default: Optional[str] = None) -> Optional[str]:
        lower = {c.lower(): c for c in columns}
        for c in candidates:
            if c.lower() in lower:
                return lower[c.lower()]
        return default

    def _get_char_max_length(self, cursor, table: str, column: str) -> Optional[int]:
        """获取字符列的最大长度（NVARCHAR/CHAR/VARCHAR），非字符列返回 None"""
        cursor.execute("""
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME=? AND COLUMN_NAME=?
        """, (table, column))
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def _hash_password(self, plain: str) -> str:
        return hashlib.sha256(plain.encode("utf-8")).hexdigest()

    # ============== 用户管理 ==============
    def list_users(self, kw: str = "") -> List[Dict[str, Any]]:
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.USER_TABLE)

            id_col = self._pick_col(cols, [self.USER_ID_COL], self.USER_ID_COL)
            name_col = self._pick_col(cols, [self.USER_NAME_COL], self.USER_NAME_COL)
            real_col = self._pick_col(cols, self.USER_REALNAME_CAND)
            role_col = self._pick_col(cols, self.USER_ROLE_CAND)
            enabled_col = self._pick_col(cols, self.USER_ENABLED_CAND)
            created_col = self._pick_col(cols, self.USER_CREATED_AT_CAND)

            select_cols = [id_col, name_col]
            if real_col: select_cols.append(real_col)
            if role_col: select_cols.append(role_col)
            if enabled_col: select_cols.append(enabled_col)
            if created_col: select_cols.append(created_col)

            base_sql = f"SELECT {', '.join(select_cols)} FROM {self.USER_TABLE}"
            params: List[Any] = []
            if kw:
                like = f"%{kw}%"
                where_parts = [f"{name_col} LIKE ?"]
                params.append(like)
                if real_col:
                    where_parts.append(f"{real_col} LIKE ?")
                    params.append(like)
                base_sql += " WHERE " + " OR ".join(where_parts)
            base_sql += f" ORDER BY {id_col} DESC"

            cur.execute(base_sql, params)
            rows = cur.fetchall()

            results: List[Dict[str, Any]] = []
            for r in rows:
                idx = 0
                item: Dict[str, Any] = {}
                item["id"] = r[idx]; idx += 1
                item["username"] = r[idx]; idx += 1
                if real_col:
                    item["real_name"] = r[idx]; idx += 1
                if role_col:
                    item["role"] = r[idx]; idx += 1
                if enabled_col:
                    item["enabled"] = bool(r[idx]); idx += 1
                if created_col:
                    val = r[idx]
                    item["created_at"] = val.strftime("%Y-%m-%d %H:%M:%S") if hasattr(val, "strftime") else (str(val) if val is not None else None)
                    idx += 1
                results.append(item)
            return results

    def create_user(self, data: Dict[str, Any]) -> int:
        """
        data: {username, real_name, role, enabled(bool), password}
        - 若密码列长度 < 60 或表名为 SysUser => 写明文；
        - 否则写 SHA-256。
        """
        username = (data.get("username") or "").strip()
        if not username:
            raise ValueError("用户名不能为空")
        password = (data.get("password") or "").strip()
        if not password:
            raise ValueError("新增用户必须设置密码")

        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.USER_TABLE)

            name_col = self._pick_col(cols, [self.USER_NAME_COL], self.USER_NAME_COL)
            real_col = self._pick_col(cols, self.USER_REALNAME_CAND)
            role_col = self._pick_col(cols, self.USER_ROLE_CAND)
            enabled_col = self._pick_col(cols, self.USER_ENABLED_CAND)
            pwd_col = self._pick_col(cols, self.USER_PWD_CAND)
            created_col = self._pick_col(cols, self.USER_CREATED_AT_CAND)

            insert_cols: List[str] = [name_col]
            values: List[Any] = [username]

            if real_col:
                insert_cols.append(real_col); values.append(data.get("real_name"))
            if role_col:
                insert_cols.append(role_col); values.append(data.get("role") or "user")
            if enabled_col:
                insert_cols.append(enabled_col); values.append(1 if data.get("enabled") else 0)
            if pwd_col:
                # 决定是否使用哈希
                maxlen = self._get_char_max_length(cur, self.USER_TABLE, pwd_col)
                use_plain = (self.USER_TABLE.lower() == "sysuser") or (maxlen is not None and maxlen < 60)
                insert_cols.append(pwd_col)
                values.append(password if use_plain else self._hash_password(password))
            if created_col:
                insert_cols.append(created_col); values.append(datetime.now())

            placeholders = ", ".join(["?"] * len(insert_cols))
            sql = f"INSERT INTO {self.USER_TABLE} ({', '.join(insert_cols)}) VALUES ({placeholders}); SELECT SCOPE_IDENTITY();"
            cur.execute(sql, values)
            row = cur.fetchone()
            new_id = int(row[0]) if row and row[0] is not None else 0
            conn.commit()
            return new_id

    def update_user(self, user_id: int, data: Dict[str, Any]) -> None:
        """
        data 可包含：username, real_name, role, enabled(bool), password(留空则不改)
        """
        if not user_id:
            raise ValueError("缺少用户ID")

        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.USER_TABLE)

            set_parts: List[str] = []
            values: List[Any] = []

            if self._pick_col(cols, [self.USER_NAME_COL]) and ("username" in data) and (data["username"] is not None):
                set_parts.append(f"{self.USER_NAME_COL} = ?")
                values.append(data["username"])

            real_col = self._pick_col(cols, self.USER_REALNAME_CAND)
            if real_col and ("real_name" in data):
                set_parts.append(f"{real_col} = ?")
                values.append(data["real_name"])

            role_col = self._pick_col(cols, self.USER_ROLE_CAND)
            if role_col and ("role" in data):
                set_parts.append(f"{role_col} = ?")
                values.append(data["role"])

            enabled_col = self._pick_col(cols, self.USER_ENABLED_CAND)
            if enabled_col and ("enabled" in data):
                set_parts.append(f"{enabled_col} = ?")
                values.append(1 if data["enabled"] else 0)

            pwd_col = self._pick_col(cols, self.USER_PWD_CAND)
            if pwd_col and data.get("password"):
                maxlen = self._get_char_max_length(cur, self.USER_TABLE, pwd_col)
                use_plain = (self.USER_TABLE.lower() == "sysuser") or (maxlen is not None and maxlen < 60)
                set_parts.append(f"{pwd_col} = ?")
                values.append(data["password"] if use_plain else self._hash_password(data["password"]))

            if not set_parts:
                return  # 没有可更新列

            id_col = self._pick_col(cols, [self.USER_ID_COL], self.USER_ID_COL)
            sql = f"UPDATE {self.USER_TABLE} SET {', '.join(set_parts)} WHERE {id_col} = ?"
            values.append(user_id)
            cur.execute(sql, values)
            conn.commit()

    def delete_user(self, user_id: int) -> None:
        if not user_id:
            return
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.USER_TABLE)
            id_col = self._pick_col(cols, [self.USER_ID_COL], self.USER_ID_COL)
            cur.execute(f"DELETE FROM {self.USER_TABLE} WHERE {id_col} = ?", (user_id,))
            conn.commit()

    # ============== 返还方式 ==============
    def list_refund_methods(self) -> List[Dict[str, Any]]:
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.REFUND_TABLE)
            id_col = self._pick_col(cols, [self.REFUND_ID_COL], self.REFUND_ID_COL)
            name_col = self._pick_col(cols, self.REFUND_NAME_CAND)
            if not name_col:
                raise RuntimeError("Mujufanhuan 表缺少名称列")
            sql = f"SELECT {id_col}, {name_col} FROM {self.REFUND_TABLE} ORDER BY {id_col} DESC"
            cur.execute(sql)
            return [{"id": r[0], "name": r[1]} for r in cur.fetchall()]

    def create_refund_method(self, name: str) -> int:
        if not name.strip():
            raise ValueError("返还方式名称不能为空")
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.REFUND_TABLE)
            name_col = self._pick_col(cols, self.REFUND_NAME_CAND)
            if not name_col:
                raise RuntimeError("Mujufanhuan 表缺少名称列")
            sql = f"INSERT INTO {self.REFUND_TABLE} ({name_col}) VALUES (?); SELECT SCOPE_IDENTITY();"
            cur.execute(sql, (name.strip(),))
            row = cur.fetchone()
            new_id = int(row[0]) if row and row[0] is not None else 0
            conn.commit()
            return new_id

    def update_refund_method(self, rid: int, name: str) -> None:
        if not rid:
            return
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.REFUND_TABLE)
            id_col = self._pick_col(cols, [self.REFUND_ID_COL], self.REFUND_ID_COL)
            name_col = self._pick_col(cols, self.REFUND_NAME_CAND)
            sql = f"UPDATE {self.REFUND_TABLE} SET {name_col} = ? WHERE {id_col} = ?"
            cur.execute(sql, (name.strip(), rid))
            conn.commit()

    def delete_refund_method(self, rid: int) -> None:
        if not rid:
            return
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.REFUND_TABLE)
            id_col = self._pick_col(cols, [self.REFUND_ID_COL], self.REFUND_ID_COL)
            cur.execute(f"DELETE FROM {self.REFUND_TABLE} WHERE {id_col} = ?", (rid,))
            conn.commit()

    # ============== 模具工艺 ==============
    def list_process_methods(self) -> List[Dict[str, Any]]:
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.PROC_TABLE)
            id_col = self._pick_col(cols, [self.PROC_ID_COL], self.PROC_ID_COL)
            name_col = self._pick_col(cols, self.PROC_NAME_CAND)
            if not name_col:
                raise RuntimeError("MJGYi 表缺少名称列")
            sql = f"SELECT {id_col}, {name_col} FROM {self.PROC_TABLE} ORDER BY {id_col} DESC"
            cur.execute(sql)
            return [{"id": r[0], "name": r[1]} for r in cur.fetchall()]

    def create_process_method(self, name: str) -> int:
        if not name.strip():
            raise ValueError("工艺名称不能为空")
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.PROC_TABLE)
            name_col = self._pick_col(cols, self.PROC_NAME_CAND)
            if not name_col:
                raise RuntimeError("MJGYi 表缺少名称列")
            sql = f"INSERT INTO {self.PROC_TABLE} ({name_col}) VALUES (?); SELECT SCOPE_IDENTITY();"
            cur.execute(sql, (name.strip(),))
            row = cur.fetchone()
            new_id = int(row[0]) if row and row[0] is not None else 0
            conn.commit()
            return new_id

    def update_process_method(self, pid: int, name: str) -> None:
        if not pid:
            return
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.PROC_TABLE)
            id_col = self._pick_col(cols, [self.PROC_ID_COL], self.PROC_ID_COL)
            name_col = self._pick_col(cols, self.PROC_NAME_CAND)
            sql = f"UPDATE {self.PROC_TABLE} SET {name_col} = ? WHERE {id_col} = ?"
            cur.execute(sql, (name.strip(), pid))
            conn.commit()

    def delete_process_method(self, pid: int) -> None:
        if not pid:
            return
        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.PROC_TABLE)
            id_col = self._pick_col(cols, [self.PROC_ID_COL], self.PROC_ID_COL)
            cur.execute(f"DELETE FROM {self.PROC_TABLE} WHERE {id_col} = ?", (pid,))
            conn.commit()

    # ============== 模具与物料对照（分页 + 查询） ==============
    def list_mjwldzhao(self, kw: str = "", page: int = 1, size: int = 20) -> Tuple[List[Dict[str, Any]], int]:
        page = max(1, int(page or 1))
        size = max(1, min(int(size or 20), 200))

        with get_dst_connection() as conn:
            cur = conn.cursor()
            cols = self._get_columns(cur, self.MAP_TABLE)

            mjid_col = self._pick_col(cols, self.MAP_MJID_CAND)
            mjname_col = self._pick_col(cols, self.MAP_MJNAME_CAND)
            cinvcode_col = self._pick_col(cols, self.MAP_CINVCODE_CAND)
            cinvname_col = self._pick_col(cols, self.MAP_CINVNAME_CAND)

            select_cols = [c for c in [mjid_col, mjname_col, cinvcode_col, cinvname_col] if c]
            if not select_cols:
                raise RuntimeError("MJWLDZhao 表缺少必要列（MJ_id/MJ_name/cinvcode/cinvname）")

            # WHERE
            where_sql: str = ""
            params: List[Any] = []
            if kw:
                like = f"%{kw}%"
                parts: List[str] = []
                for col in select_cols:
                    parts.append(f"{col} LIKE ?")
                    params.append(like)
                where_sql = " WHERE " + " OR ".join(parts)

            # 总数
            count_sql = f"SELECT COUNT(1) FROM {self.MAP_TABLE}" + where_sql
            cur.execute(count_sql, params)
            row = cur.fetchone()
            total = int(row[0]) if row and row[0] is not None else 0

            # 列表（分页）
            offset = (page - 1) * size
            list_sql = f"""
            SELECT {', '.join(select_cols)}
            FROM {self.MAP_TABLE}
            {where_sql}
            ORDER BY {select_cols[0]} DESC
            OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
            """
            cur.execute(list_sql, (*params, offset, size))
            rows = cur.fetchall()

            result: List[Dict[str, Any]] = []
            for r in rows:
                idx = 0
                item: Dict[str, Any] = {}
                if mjid_col:    item["mj_id"] = r[idx]; idx += 1
                if mjname_col:  item["mj_name"] = r[idx]; idx += 1
                if cinvcode_col:item["cinvcode"] = r[idx]; idx += 1
                if cinvname_col:item["cinvname"] = r[idx]; idx += 1
                result.append(item)

            return result, total
