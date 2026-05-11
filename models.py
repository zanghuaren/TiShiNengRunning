"""
SQLAlchemy 数据库模型定义
"""
import uuid

from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, Index, UniqueConstraint, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base


class RunPath(Base):
    """跑步路径记录表"""
    __tablename__ = "tsn_run_path"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_path_id = Column(String(100), unique=True, nullable=False, index=True, comment="跑步路径ID")
    school_code = Column(String(50), nullable=False, index=True, comment="学校代码")
    sport_range = Column(Float, nullable=False, comment="运动范围/距离")
    run_line_path = Column(Text, nullable=False, comment="跑步路线(JSON格式的GeoJSON)")
    point_id_list = Column(Text, nullable=True, comment="打卡点ID列表(JSON格式)")
    ok_point_list_json = Column(Text, nullable=True, comment="已打卡点列表(JSON格式)")
    is_public = Column(Boolean, default=True, comment="是否公开")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), comment="更新时间")

    # 创建复合索引
    __table_args__ = (
        Index('idx_school_code_run_path_id', 'school_code', 'run_path_id'),
    )

    def __repr__(self):
        return f"<RunPath(id={self.id}, run_path_id={self.run_path_id}, school_code={self.school_code})>"


def getUUID4Str():
    return str(uuid.uuid4())


class TsnAccount_Model(Base):
    __tablename__ = 'tsn_account'
    __table_args__ = (
        UniqueConstraint('user_id', 'school_id', name='user_id_school_id_unique'),
    )

    id = Column(Integer, primary_key=True)
    student_id = Column(String(64))
    user_id = Column(String(64), index=True)

    school_id = Column(Integer, ForeignKey('tsn_school.school_id'), nullable=False)
    school = relationship("TsnSchool_Model", overlaps="accounts")

    username = Column(String(64), nullable=False)
    password = Column(String(64), nullable=False)

    mobile_device_id = Column(String(128), default=getUUID4Str, nullable=False)

    access_token = Column(String(128), nullable=False)
    refresh_token = Column(String(128), nullable=False)
    expires_in = Column(Integer, nullable=False)

    auth_code = Column(String(128), default=getUUID4Str)
    managed_by = Column(String(64), nullable=True, index=True)


class TsnSchool_Model(Base):
    __tablename__ = 'tsn_school'

    id = Column(Integer, primary_key=True)
    school_id = Column(Integer, unique=True)

    accounts = relationship("TsnAccount_Model", overlaps="school")

    school_name = Column(String(128), nullable=False)
    school_url = Column(String(128), nullable=False)
    lan_url = Column(String(128))
    open_id = Column(String(128), nullable=False)
    is_open_keep = Column(Boolean, default=False)
    is_open_live = Column(Boolean, default=False)
    is_open_encry = Column(Boolean, default=False)
    sys_type = Column(Integer, nullable=False)
    school_code = Column(String(128), nullable=False)

    def isPublicVersion(self):
        return self.sys_type == 2


class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('tsn_account.id'), nullable=False)
    account = relationship("TsnAccount_Model")

    plan_id = Column(Integer, ForeignKey('plans.id'), nullable=True)

    run_type = Column(String(32), nullable=False)  # morningRun | sumRun | freedom
    distance = Column(Float, nullable=False)
    status = Column(String(16), nullable=False, default='pending')  # pending | running | completed | failed
    username = Column(String(64), nullable=True)
    school_name = Column(String(128), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    started_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    error_msg = Column(Text, nullable=True)
    result_msg = Column(Text, nullable=True)
    logs = Column(Text, nullable=True)
    use_image_bed = Column(Boolean, default=False, nullable=False)
    pace = Column(Float, nullable=True)  # 分钟/公里，None 表示随机


class Plan(Base):
    __tablename__ = 'plans'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('tsn_account.id'), nullable=False)
    account = relationship("TsnAccount_Model")

    run_type = Column(String(32), nullable=False)
    total_distance = Column(Float, nullable=False)
    daily_limit = Column(Float, nullable=False)
    completed_distance = Column(Float, default=0.0, nullable=False)
    status = Column(String(16), default='active', nullable=False)  # active | completed | cancelled
    last_run_date = Column(String(10), nullable=True)  # YYYY-MM-DD
    use_image_bed = Column(Boolean, default=False, nullable=False)
    scheduled_hour = Column(Float, nullable=True)
    pace = Column(Float, nullable=True)  # 分钟/公里，None 表示随机
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class WebUser(Base):
    """Web 管理后台用户表"""
    __tablename__ = "web_users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    hashed_password = Column(String(128), nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    credits = Column(Float, nullable=False, default=0.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
