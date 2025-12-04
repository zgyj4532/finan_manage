# database_setup.py - 表结构与项目2完全一致
import logging
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import QueuePool
from config import DB_CONFIG, PLATFORM_MERCHANT_ID, MEMBER_PRODUCT_PRICE

logger = logging.getLogger(__name__)

_engine = None
_SessionFactory = None

def get_engine():
    global _engine
    if _engine is None:
        try:
            connection_url = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
                f"?charset={DB_CONFIG['charset']}"
            )
            _engine = create_engine(
                connection_url,
                poolclass=QueuePool,
                pool_size=20,
                max_overflow=30,
                pool_timeout=30,
                pool_pre_ping=True,
                echo=False
            )
            logger.info("✅ SQLAlchemy 引擎已创建 (pool_size=20)")
        except Exception as e:
            logger.error(f"❌ SQLAlchemy 引擎创建失败: {e}")
            raise
    return _engine

def get_session_factory():
    global _SessionFactory
    if _SessionFactory is None:
        engine = get_engine()
        _SessionFactory = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False
        )
        logger.info("✅ 会话工厂已创建")
    return _SessionFactory

def get_db_session():
    factory = get_session_factory()
    db = scoped_session(factory)()
    try:
        yield db
    finally:
        db.close()

class DatabaseManager:
    def __init__(self):
        self._ensure_database_exists()

    def _ensure_database_exists(self):
        try:
            temp_config = DB_CONFIG.copy()
            database = temp_config.pop('database')
            import pymysql
            conn = pymysql.connect(**temp_config)
            cursor = conn.cursor()
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{database}` "
                f"DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn.commit()
            conn.close()
            logger.info(f"✅ 数据库 `{database}` 已就绪")
        except Exception as e:
            logger.error(f"❌ 数据库初始化失败: {e}")
            raise

    def init_all_tables(self, conn):
        logger.info("\n=== 初始化数据库表结构 ===")

        tables = {
            'users': """
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    mobile VARCHAR(30) UNIQUE NOT NULL,
                    password_hash CHAR(60) NOT NULL,
                    name VARCHAR(50) NOT NULL,
                    member_level TINYINT NOT NULL DEFAULT 0,
                    points BIGINT NOT NULL DEFAULT 0,
                    promotion_balance DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    merchant_points BIGINT NOT NULL DEFAULT 0,
                    merchant_balance DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    status TINYINT NOT NULL DEFAULT 1,
                    level_changed_at DATETIME NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_mobile (mobile),
                    INDEX idx_member_level (member_level)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'products': """
                CREATE TABLE IF NOT EXISTS products (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    sku VARCHAR(64) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(12,2) NOT NULL,
                    stock INT NOT NULL DEFAULT 0,
                    is_member_product TINYINT(1) NOT NULL DEFAULT 0,
                    status TINYINT NOT NULL DEFAULT 1,
                    merchant_id BIGINT UNSIGNED NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_is_member_product (is_member_product),
                    INDEX idx_merchant (merchant_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'orders': """
                CREATE TABLE IF NOT EXISTS orders (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    order_no VARCHAR(64) UNIQUE NOT NULL,
                    user_id BIGINT UNSIGNED NOT NULL,
                    merchant_id BIGINT UNSIGNED NOT NULL,
                    total_amount DECIMAL(12,2) NOT NULL,
                    original_amount DECIMAL(12,2) NOT NULL,
                    points_discount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    is_member_order TINYINT(1) NOT NULL DEFAULT 0,
                    status VARCHAR(30) NOT NULL DEFAULT 'completed',
                    refund_status VARCHAR(30) DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_user (user_id),
                    INDEX idx_order_no (order_no),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'order_items': """
                CREATE TABLE IF NOT EXISTS order_items (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    order_id BIGINT UNSIGNED NOT NULL,
                    product_id BIGINT UNSIGNED NOT NULL,
                    quantity INT NOT NULL DEFAULT 1,
                    unit_price DECIMAL(12,2) NOT NULL,
                    total_price DECIMAL(12,2) NOT NULL,
                    INDEX idx_order (order_id),
                    INDEX idx_product (product_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'finance_accounts': """
                CREATE TABLE IF NOT EXISTS finance_accounts (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    account_name VARCHAR(100) NOT NULL,
                    account_type VARCHAR(50) UNIQUE NOT NULL,
                    balance DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_account_type (account_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'account_flow': """
                CREATE TABLE IF NOT EXISTS account_flow (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    account_id BIGINT UNSIGNED,
                    related_user BIGINT UNSIGNED,
                    account_type VARCHAR(50),
                    change_amount DECIMAL(14,2) NOT NULL,
                    balance_after DECIMAL(14,2),
                    flow_type VARCHAR(50),
                    remark VARCHAR(255),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_account (account_id),
                    INDEX idx_related_user (related_user),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'points_log': """
                CREATE TABLE IF NOT EXISTS points_log (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    change_amount BIGINT NOT NULL,
                    balance_after BIGINT NOT NULL,
                    type ENUM('member','merchant') NOT NULL,
                    reason VARCHAR(255),
                    related_order BIGINT UNSIGNED,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user (user_id),
                    INDEX idx_order (related_order)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'user_referrals': """
                CREATE TABLE IF NOT EXISTS user_referrals (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED UNIQUE NOT NULL,
                    referrer_id BIGINT UNSIGNED,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_referrer (referrer_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'pending_rewards': """
                CREATE TABLE IF NOT EXISTS pending_rewards (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    reward_type ENUM('referral','team') NOT NULL,
                    amount DECIMAL(12,2) NOT NULL,
                    order_id BIGINT UNSIGNED NOT NULL,
                    layer TINYINT DEFAULT NULL,
                    status ENUM('pending','approved','rejected') DEFAULT 'pending',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_status (user_id, status),
                    INDEX idx_order_id (order_id),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'coupons': """
                CREATE TABLE IF NOT EXISTS coupons (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    coupon_type ENUM('user','merchant') NOT NULL,
                    amount DECIMAL(14,2) NOT NULL,
                    status ENUM('unused','used','expired') NOT NULL DEFAULT 'unused',
                    valid_from DATE NOT NULL,
                    valid_to DATE NOT NULL,
                    used_at DATETIME DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_status (user_id, status),
                    INDEX idx_valid_to (valid_to)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'withdrawals': """
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    amount DECIMAL(14,2) NOT NULL,
                    tax_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    actual_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    status VARCHAR(30) NOT NULL DEFAULT 'pending_auto',
                    audit_remark VARCHAR(255) DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    processed_at DATETIME DEFAULT NULL,
                    INDEX idx_user_status (user_id, status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'team_rewards': """
                CREATE TABLE IF NOT EXISTS team_rewards (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    from_user_id BIGINT UNSIGNED NOT NULL,
                    order_id BIGINT UNSIGNED,
                    layer TINYINT NOT NULL,
                    reward_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id),
                    INDEX idx_from_user_id (from_user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'weekly_subsidy_records': """
                CREATE TABLE IF NOT EXISTS weekly_subsidy_records (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    week_start DATE NOT NULL,
                    subsidy_amount DECIMAL(14,2) NOT NULL,
                    points_before BIGINT NOT NULL,
                    points_deducted BIGINT NOT NULL,
                    coupon_id BIGINT UNSIGNED,
                    INDEX idx_user_week (user_id, week_start)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            'director_dividends': """
                CREATE TABLE IF NOT EXISTS director_dividends (
                    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                    user_id BIGINT UNSIGNED NOT NULL,
                    period_date DATE NOT NULL,
                    dividend_amount DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                    status VARCHAR(30) NOT NULL DEFAULT 'pending',
                    paid_at DATETIME DEFAULT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_user_id (user_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
        }

        for table_name, sql in tables.items():
            conn.execute(text(sql))
            logger.info(f"✅ 表 `{table_name}` 已创建/确认")

        self._init_finance_accounts(conn)
        logger.info("✅ 所有表结构初始化完成")

    def _init_finance_accounts(self, conn):
        accounts = [
            ('周补贴池', 'subsidy_pool'),
            ('公益基金', 'public_welfare'),
            ('平台维护', 'platform'),
            ('荣誉董事分红', 'honor_director'),
            ('社区店', 'community'),
            ('城市运营中心', 'city_center'),
            ('大区分公司', 'region_company'),
            ('事业发展基金', 'development'),
            ('公司积分账户', 'company_points'),
            ('公司余额账户', 'company_balance'),
            ('平台收入池（会员商品）', 'platform_revenue_pool'),
        ]

        conn.execute(text("DELETE FROM finance_accounts"))
        for name, acc_type in accounts:
            conn.execute(
                text("INSERT INTO finance_accounts (account_name, account_type, balance) VALUES (:name, :type, 0)"),
                {"name": name, "type": acc_type}
            )
        logger.info(f"✅ 初始化 {len(accounts)} 个资金池账户")

    def create_test_data(self, conn) -> int:
        logger.info("\n--- 创建测试数据 ---")

        pwd_hash = '$2b$12$9LjsHS5r4u1M9K4nG5KZ7e6zZxZn7qZ'

        result = conn.execute(
            text("INSERT INTO users (mobile, password_hash, name, status) VALUES (:mobile, :pwd, :name, 1)"),
            {"mobile": '13800138004', "pwd": pwd_hash, "name": '优质商家'}
        )
        merchant_id = result.lastrowid

        conn.execute(
            text("""INSERT INTO products (sku, name, price, stock, is_member_product, merchant_id, status)
                    VALUES (:sku, :name, :price, 100, 1, :merchant_id, 1)"""),
            {"sku": 'SKU-MEMBER-001', "name": '会员星卡', "price": float(MEMBER_PRODUCT_PRICE), "merchant_id": PLATFORM_MERCHANT_ID}
        )

        conn.execute(
            text("""INSERT INTO products (sku, name, price, stock, is_member_product, merchant_id, status)
                    VALUES (:sku, :name, 500.00, 200, 0, :merchant_id, 1)"""),
            {"sku": 'SKU-NORMAL-001', "name": '普通商品', "merchant_id": merchant_id}
        )

        conn.commit()
        logger.info(f"✅ 测试数据创建完成 | 商家ID: {merchant_id}")
        return merchant_id