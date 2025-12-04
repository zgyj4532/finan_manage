# config.py - 与项目2逻辑完全对齐
from decimal import Decimal
from enum import StrEnum, IntEnum
from typing import Final
import os
from dotenv import load_dotenv

load_dotenv()

# 数据库配置
DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'charset': 'utf8mb4',
}

# 平台常量
PLATFORM_MERCHANT_ID: Final[int] = 0
MEMBER_PRODUCT_PRICE: Final[Decimal] = Decimal('1980.00')

# 业务规则枚举
class AllocationKey(StrEnum):
    PUBLIC_WELFARE = 'public_welfare'
    PLATFORM = 'platform'
    SUBSIDY_POOL = 'subsidy_pool'
    HONOR_DIRECTOR = 'honor_director'
    COMMUNITY = 'community'
    CITY_CENTER = 'city_center'
    REGION_COMPANY = 'region_company'
    DEVELOPMENT = 'development'
    PLATFORM_REVENUE_POOL = 'platform_revenue_pool'
    COMPANY_POINTS = 'company_points'
    COMPANY_BALANCE = 'company_balance'

# 资金分配比例
ALLOCATIONS: Final[dict[AllocationKey, Decimal]] = {
    AllocationKey.PUBLIC_WELFARE: Decimal('0.01'),
    AllocationKey.PLATFORM: Decimal('0.01'),
    AllocationKey.SUBSIDY_POOL: Decimal('0.12'),
    AllocationKey.HONOR_DIRECTOR: Decimal('0.02'),
    AllocationKey.COMMUNITY: Decimal('0.01'),
    AllocationKey.CITY_CENTER: Decimal('0.01'),
    AllocationKey.REGION_COMPANY: Decimal('0.005'),
    AllocationKey.DEVELOPMENT: Decimal('0.015'),
}

# 其他业务常量
MAX_POINTS_VALUE: Final[Decimal] = Decimal('0.02')
TAX_RATE: Final[Decimal] = Decimal('0.06')
POINTS_DISCOUNT_RATE: Final[Decimal] = Decimal('1.0')
COUPON_VALID_DAYS: Final[int] = 30
MAX_PURCHASE_PER_DAY: Final[int] = 2
MAX_TEAM_LAYER: Final[int] = 6

# 用户状态
class UserStatus(IntEnum):
    NORMAL = 1
    HONOR_DIRECTOR = 9

# 奖励类型
class RewardType(StrEnum):
    REFERRAL = 'referral'
    TEAM = 'team'

class RewardStatus(StrEnum):
    PENDING = 'pending'
    APPROVED = 'approved'
    REJECTED = 'rejected'

# 优惠券类型
class CouponType(StrEnum):
    USER = 'user'
    MERCHANT = 'merchant'

class CouponStatus(StrEnum):
    UNUSED = 'unused'
    USED = 'used'
    EXPIRED = 'expired'

# 提现状态
class WithdrawalStatus(StrEnum):
    PENDING_AUTO = 'pending_auto'
    PENDING_MANUAL = 'pending_manual'
    APPROVED = 'approved'
    REJECTED = 'rejected'

# 订单状态
class OrderStatus(StrEnum):
    COMPLETED = 'completed'
    REFUNDED = 'refunded'

# 日志配置
LOG_DIR: Final[str] = os.path.join(os.path.dirname(__file__), 'logs')
LOG_FILE: Final[str] = os.path.join(LOG_DIR, 'finance.log')
os.makedirs(LOG_DIR, exist_ok=True)