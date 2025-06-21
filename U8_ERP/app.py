# app.py
"""
主程序入口
- 初始化Flask应用
- 注册所有业务相关的蓝图（API路由模块）
- 支持全局配置和后续中间件扩展
"""

from flask import Flask

# 导入API蓝图（注意：需在/api/目录下先创建好相关py文件，并在此处导入）
from api.user_api import user_api            # 用户与权限接口
from api.sync_api import sync_api          # U8主数据同步接口（后续补全）
from api.mrp_api import mrp_api            # MRP/生产订单/BOM接口（后续补全）
from api.purchase_api import purchase_api  # 采购请购单接口（后续补全）
from api.mold_api import mold_api          # 模具管理接口（后续补全）

def create_app():
    """
    工厂方法，创建并配置Flask应用
    可方便扩展多环境/测试场景
    """
    app = Flask(__name__)

    # 注册各业务模块API蓝图
    app.register_blueprint(user_api, url_prefix='/api/user')           # 用户相关接口
    app.register_blueprint(sync_api, url_prefix='/api/sync')         # U8同步
    app.register_blueprint(mrp_api, url_prefix='/api/mrp')           # MRP运算
    app.register_blueprint(purchase_api, url_prefix='/api/purchase') # 采购请购单
    app.register_blueprint(mold_api, url_prefix='/api/mold')         # 模具管理

    # 如有CORS、Session、全局异常等全局中间件，可在此统一添加
    # from flask_cors import CORS
    # CORS(app)

    return app

# 支持命令行启动
if __name__ == '__main__':
    app = create_app()
    # debug=True便于开发调试，生产环境请关闭
    app.run(host='0.0.0.0', port=5000, debug=True)
