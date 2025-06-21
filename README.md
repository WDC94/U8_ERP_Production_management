# U8-ERP-生产管理辅助系统
本项目主要涉及U8 ERP系统MRP计算、根据计算结果写入请购单、模具管理等功能

# 一、后端主目录结构
 project_root/  
 ├─ app.py                  # 主程序入口  
 ├─ config.py               # 配置文件（数据库/账套参数）  
 ├─ requirements.txt        # 依赖库清单  
 ├─ /modules/               # 业务核心模块  
 │    ├─ user.py            # 用户/权限/操作日志  
 │    ├─ sync.py            # U8数据同步与只读接口  
 │    ├─ mrp.py             # 生产计划/MRP/BOM运算  
 │    ├─ purchase.py        # 采购请购单处理  
 │    ├─ mold.py            # 模具管理  
 │    └─ utils.py           # 工具/日志/参数/异常  
 ├─ /db/  
 │    ├─ models.py          # ORM模型定义（表结构对应）  
 │    └─ session.py         # 数据库会话管理  
 ├─ /api/                   # RESTful接口蓝图  
 │    ├─ user_api.py     
 │    ├─ sync_api.py  
 │    ├─ mrp_api.py  
 │    ├─ purchase_api.py  
 │    └─ mold_api.py  
 ├─ /jobs/                  # 定时任务脚本（同步/运算等）  
 ├─ /logs/                  # 各类操作/业务/异常日志  
 ├─ /templates/             # 前端模板  
 ├─ /static/                # 静态资源  
 └─ /tests/                 # 自动化测试  

# 二、主要模块说明与分工
1. modules/
user.py：注册、登录、权限判定、角色分配、用户操作日志、账号启用/禁用。
sync.py：U8基础数据的读取（物料/BOM/供应商/库存），只读接口，支持定时/手动同步。
mrp.py：生产订单运算，按BOM递归分解、MRP需求、库存展望、分组拆分、导出结果。
purchase.py：MRP结果导入、自动生成采购请购单、请购表写入U8、异常记录导出。
mold.py：模具台账、调拨、期间记录、与U8供应商/存货档案的联动、不可删校验。
utils.py：数据库连接、配置、通用日志、文件导入导出、异常捕获等工具函数。

3. db/
models.py：所有表（参考你的建表SQL建表语句）的ORM模型（可用SQLAlchemy/Peewee）。
session.py：数据库会话管理，负责连接池与统一入口（所有数据操作模块通过它访问数据库）。

3. api/
每个业务线一个蓝图/路由模块，负责HTTP接口、参数校验、数据序列化、异常处理等：
user_api.py：用户注册、登录、查询、权限、操作日志接口
sync_api.py：U8主数据同步、账套切换、同步进度/异常
mrp_api.py：生产订单/MRP/BOM运算、拆分、查询、导出
purchase_api.py：请购单生成、写U8、查询、异常导出
mold_api.py：模具台账、调拨、期间、与U8联动

4. jobs/
APScheduler/Celery等定时任务脚本：比如定时同步U8主数据，自动批量生成请购单。

5. logs/
操作日志、同步日志、异常日志等，可落地为数据库或文件，方便追溯。
