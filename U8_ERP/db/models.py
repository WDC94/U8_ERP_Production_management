# db/models.py
"""
数据库ORM模型定义（使用SQLAlchemy）
- 覆盖所有核心表：Inventory、AQKCB、MRPYSJG、Supplier、Mold、MoldPeriodRecord、MoldTransferRecord、SysUser
- 字段与外键约束完全对应建表SQL
- 推荐与数据库迁移工具（如Alembic）配合使用
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Date, DECIMAL, ForeignKey, Boolean, NVARCHAR
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

class Inventory(Base):
    """
    存货档案（物料主数据表）
    """
    __tablename__ = 'Inventory'
    cInvCode = Column(NVARCHAR(50), primary_key=True, comment='物料编码')
    cInvName = Column(NVARCHAR(100), comment='物料名称')

class AQKCB(Base):
    """
    安全库存表
    """
    __tablename__ = 'AQKCB'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增主键')
    cinvcode = Column(NVARCHAR(50), ForeignKey('Inventory.cInvCode'), nullable=False, comment='物料编码')
    Lowest_iSafeNum = Column(DECIMAL(18,2), nullable=False, comment='安全库存数量')
    last_update = Column(DateTime, default=datetime.datetime.now, nullable=False, comment='更新时间')
    remark = Column(NVARCHAR(255), comment='备注')

    inventory = relationship("Inventory")

class MRPYSJG(Base):
    """
    MRP运算结果表
    """
    __tablename__ = 'MRPYSJG'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增主键')
    cinvcode = Column(NVARCHAR(50), ForeignKey('Inventory.cInvCode'), nullable=False, comment='物料编码')
    cinvname = Column(NVARCHAR(100), comment='物料名称（冗余）')
    Total_demand = Column(DECIMAL(18,2), nullable=False, comment='总需求')
    dRequirDate = Column(Date, nullable=False, comment='计划需求日')
    AS_iQuantity = Column(DECIMAL(18,2), comment='澳升库存')
    CF_iQuantity = Column(DECIMAL(18,2), comment='长帆库存')
    created_time = Column(DateTime, default=datetime.datetime.now, nullable=False, comment='记录生成时间')
    remark = Column(NVARCHAR(255), comment='备注')

    inventory = relationship("Inventory")

class Supplier(Base):
    """
    供应商表
    """
    __tablename__ = 'Supplier'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='供应商ID')
    supplier_name = Column(NVARCHAR(100), nullable=False, comment='供应商名称')
    contact = Column(NVARCHAR(50), comment='联系人')
    phone = Column(NVARCHAR(30), comment='联系方式')
    remark = Column(NVARCHAR(255), comment='备注')

class Mold(Base):
    """
    模具表
    """
    __tablename__ = 'Mold'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='模具ID')
    mold_name = Column(NVARCHAR(100), nullable=False, comment='模具名称')
    supplier_id = Column(Integer, ForeignKey('Supplier.id'), nullable=False, comment='供应商ID')
    quantity = Column(Integer, comment='数量')
    total_amount = Column(DECIMAL(18,2), comment='金额')
    create_date = Column(Integer, comment='创建日期')  # 如实际为Date请改为Date型
    remark = Column(NVARCHAR(255), comment='备注')

    supplier = relationship("Supplier")

class MoldPeriodRecord(Base):
    """
    模具期间记录表
    """
    __tablename__ = 'MoldPeriodRecord'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增主键')
    mold_id = Column(Integer, ForeignKey('Mold.id'), nullable=False, comment='模具ID')
    supplier_id = Column(Integer, ForeignKey('Supplier.id'), nullable=False, comment='供应商ID')
    period = Column(NVARCHAR(20), comment='期间')
    amount = Column(DECIMAL(18,2), comment='金额')
    note = Column(NVARCHAR(255), comment='备注')

    mold = relationship("Mold")
    supplier = relationship("Supplier")

class MoldTransferRecord(Base):
    """
    模具调拨记录表
    """
    __tablename__ = 'MoldTransferRecord'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='自增主键')
    mold_id = Column(Integer, ForeignKey('Mold.id'), nullable=False, comment='模具ID')
    from_supplier_id = Column(Integer, ForeignKey('Supplier.id'), comment='调出供应商ID')
    to_supplier_id = Column(Integer, ForeignKey('Supplier.id'), comment='调入供应商ID')
    transfer_date = Column(Date, comment='调拨日期')
    quantity = Column(Integer, comment='数量')
    note = Column(NVARCHAR(255), comment='备注')

    mold = relationship("Mold")
    from_supplier = relationship("Supplier", foreign_keys=[from_supplier_id])
    to_supplier = relationship("Supplier", foreign_keys=[to_supplier_id])

class SysUser(Base):
    """
    用户表
    """
    __tablename__ = 'SysUser'
    id = Column(Integer, primary_key=True, autoincrement=True, comment='用户ID')
    username = Column(NVARCHAR(50), unique=True, nullable=False, comment='用户名')
    password = Column(NVARCHAR(50), nullable=False, comment='明文密码')
    realname = Column(NVARCHAR(50), comment='真实姓名')
    role = Column(NVARCHAR(20), nullable=False, default='user', comment='角色')
    is_active = Column(Boolean, nullable=False, default=True, comment='是否启用')
    create_time = Column(DateTime, default=datetime.datetime.now, nullable=False, comment='创建时间')
    remark = Column(NVARCHAR(255), comment='备注')

