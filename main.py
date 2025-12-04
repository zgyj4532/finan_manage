import uvicorn
from database_setup import get_engine, DatabaseManager
from config import DB_CONFIG
from api_interface import app


def initialize_database():
    """初始化数据库表结构（如果尚未创建）"""
    print("正在检查数据库表结构...")
    engine = get_engine()
    with engine.connect() as conn:
        with conn.begin():
            db_manager = DatabaseManager()
            db_manager.init_all_tables(conn)
    print("数据库表结构初始化完成。")


def create_test_data():
    """创建测试数据（可选）"""
    print("正在创建测试数据...")
    engine = get_engine()
    with engine.connect() as conn:
        with conn.begin():
            db_manager = DatabaseManager()
            db_manager.create_test_data(conn)
    print("测试数据创建完成。")


if __name__ == "__main__":
    # 初始化数据库表结构
    initialize_database()

    # 创建测试数据（可选，仅在开发环境中使用）
    # create_test_data()

    # 启动 FastAPI 应用
    print("启动财务管理系统 API...")
    uvicorn.run(
        "api_interface:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # 开启热重载（开发模式）
        log_level="info",
        access_log=True
    )
