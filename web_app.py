#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
import random
import sys
from contextvars import ContextVar
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, status
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from loguru import logger
from passlib.context import CryptContext
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from database import get_db, init_db
from models import Order, Plan, TsnAccount_Model, RunPath, WebUser
from deviceModel import deviceModel
from services.tsnSchool.tsnSchoolDao import getSchoolListDao
from tsnClient import tsnPasswordAuthServer
from tsnRunServer import TsnRunServer, TsnRunType
from spiderServer import startSpider

app = FastAPI(title="体适能代跑管理系统")

STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
LOGS_DIR = Path(__file__).parent / "order_logs"
LOGS_DIR.mkdir(exist_ok=True)

RUN_TYPE_MAP = {
    "morningRun": TsnRunType.morningRun,
    "sumRun": TsnRunType.sumRun,
    "freedom": TsnRunType.freedom,
}
RUN_TYPE_LABELS = {"morningRun": "晨跑", "sumRun": "阳光跑", "freedom": "自由跑"}


# ── 认证配置 ──────────────────────────────────────────────────────────────────

SECRET_KEY = "bythyck.191423"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 7

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")


def _hash_password(plain: str) -> str:
    return pwd_context.hash(plain)


def _verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def _create_access_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.now(timezone.utc) + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> WebUser:
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无效或已过期的令牌",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exc
    except JWTError:
        raise credentials_exc
    async for db in get_db():
        result = await db.execute(select(WebUser).where(WebUser.username == username))
        user = result.scalar_one_or_none()
        if user is None:
            raise credentials_exc
        return user


# ── 日志路由 ──────────────────────────────────────────────────────────────────
# 每个运行中的订单有一个 asyncio.Queue，SSE 端点从中读取日志行
_current_order_id: ContextVar[Optional[int]] = ContextVar("current_order_id", default=None)
_order_log_queues: dict[int, asyncio.Queue] = {}


def _order_log_sink(message):
    try:
        order_id = _current_order_id.get()
        if order_id is not None and order_id in _order_log_queues:
            line = message.record["time"].strftime("%H:%M:%S") + " " + message.record["message"]
            _order_log_queues[order_id].put_nowait(line)
    except Exception:
        pass


# 移除默认 stderr sink，添加 stderr + 订单路由 sink
logger.remove()
logger.add(sys.stderr, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
logger.add(_order_log_sink, format="{message}", enqueue=False)


# ── 启动 ──────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    await init_db()
    async for db in get_db():
        result = await db.execute(select(WebUser).where(WebUser.username == "miss"))
        if result.scalar_one_or_none() is None:
            db.add(WebUser(username="miss", hashed_password=_hash_password("zhr"), is_admin=True))
            await db.commit()
            logger.info("已创建默认管理员账号: miss")
    # 修复上次服务异常退出遗留的孤儿订单
    async for db in get_db():
        from sqlalchemy import update
        await db.execute(
            update(Order)
            .where(Order.status.in_(["running", "pending"]))
            .values(status="failed", error_msg="服务重启导致任务中断",
                    completed_at=datetime.now())
        )
        await db.commit()
    asyncio.create_task(_plan_scheduler())


# ── 静态 ──────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


# ── 认证 ──────────────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    async for db in get_db():
        result = await db.execute(select(WebUser).where(WebUser.username == form_data.username))
        user = result.scalar_one_or_none()
        if user is None or not _verify_password(form_data.password, user.hashed_password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="用户名或密码错误",
                headers={"WWW-Authenticate": "Bearer"},
            )
        token = _create_access_token({"sub": user.username})
        return {"access_token": token, "token_type": "bearer"}


@app.get("/api/auth/me")
async def get_me(current_user: WebUser = Depends(get_current_user)):
    return {"id": current_user.id, "username": current_user.username, "is_admin": current_user.is_admin, "credits": current_user.credits}


# ── 学校 ──────────────────────────────────────────────────────────────────────

@app.get("/api/schools")
async def list_schools(current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        schools = await getSchoolListDao(db)
        return [
            {
                "school_id": s.school_id,
                "school_name": s.school_name,
                "sys_type_label": "公版" if s.sys_type == 2 else "私版",
            }
            for s in schools
        ]


# ── 账号 ──────────────────────────────────────────────────────────────────────

@app.get("/api/accounts")
async def list_accounts(current_user: WebUser = Depends(get_current_user)):
    from sqlalchemy import func as sqlfunc, case as sqcase
    async for db in get_db():
        stmt = select(TsnAccount_Model).options(selectinload(TsnAccount_Model.school))
        if not current_user.is_admin:
            stmt = stmt.where(TsnAccount_Model.managed_by == current_user.username)
        result = await db.execute(stmt)
        accounts = result.scalars().all()

        # 一次聚合查询所有账号里程
        mile_stmt = (
            select(
                Order.account_id,
                sqlfunc.coalesce(sqlfunc.sum(Order.distance), 0).label("total"),
                sqlfunc.coalesce(
                    sqlfunc.sum(sqcase((Order.status == "completed", Order.distance), else_=0)), 0
                ).label("completed"),
            )
            .where(Order.status != "failed")
            .group_by(Order.account_id)
        )
        mile_result = await db.execute(mile_stmt)
        mileage = {row.account_id: (row.total, row.completed) for row in mile_result}

        return [
            {
                "id": a.id,
                "username": a.username,
                "user_id": a.user_id,
                "school_id": a.school_id,
                "school_name": a.school.school_name if a.school else "",
                "sys_type_label": "公版" if (a.school and a.school.sys_type == 2) else "私版",
                "total_distance": mileage.get(a.id, (0, 0))[0],
                "completed_distance": mileage.get(a.id, (0, 0))[1],
            }
            for a in accounts
        ]


@app.delete("/api/accounts/{account_id}", status_code=200)
async def delete_account(account_id: int, current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        stmt = select(TsnAccount_Model).where(TsnAccount_Model.id == account_id)
        result = await db.execute(stmt)
        account = result.scalar_one_or_none()
        if account is None:
            raise HTTPException(status_code=404, detail="账号不存在")
        if not current_user.is_admin and account.managed_by != current_user.username:
            raise HTTPException(status_code=403, detail="无权删除此账号")
        # 检查是否有进行中的订单
        running_stmt = select(Order).where(
            Order.account_id == account_id,
            Order.status.in_(["pending", "running"]),
        )
        running_result = await db.execute(running_stmt)
        if running_result.scalar_one_or_none() is not None:
            raise HTTPException(status_code=400, detail="该账号有进行中的订单，无法删除")
        await db.delete(account)
        await db.commit()
        return {"id": account_id, "deleted": True}


# ── 跑步类型 ──────────────────────────────────────────────────────────────────

@app.get("/api/run-types")
async def list_run_types(current_user: WebUser = Depends(get_current_user)):
    return [{"value": k, "label": v} for k, v in RUN_TYPE_LABELS.items()]


# ── 订单工具 ──────────────────────────────────────────────────────────────────

def _fmt_dt(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S")


def _order_dict(order: Order) -> dict:
    return {
        "id": order.id,
        "account_id": order.account_id,
        "username": (order.account.username if order.account else None) or order.username or "",
        "school_name": (order.account.school.school_name if order.account and order.account.school else None) or order.school_name or "",
        "run_type": order.run_type,
        "run_type_label": RUN_TYPE_LABELS.get(order.run_type, order.run_type),
        "distance": order.distance,
        "status": order.status,
        "created_at": _fmt_dt(order.created_at),
        "started_at": _fmt_dt(order.started_at),
        "completed_at": _fmt_dt(order.completed_at),
        "error_msg": order.error_msg,
        "result_msg": order.result_msg,
    }


# ── 后台任务 ──────────────────────────────────────────────────────────────────

async def _execute_order(order_id: int, start_delay: float = 0):
    """后台协程：执行跑步任务，日志实时推送到 SSE 队列并持久化到文件"""
    if start_delay > 0:
        await asyncio.sleep(start_delay)
    _current_order_id.set(order_id)
    queue: asyncio.Queue = asyncio.Queue(maxsize=2000)
    _order_log_queues[order_id] = queue

    log_file = LOGS_DIR / f"{order_id}.log"
    log_fh = open(log_file, "w", encoding="utf-8", buffering=1)

    _NOISE_PREFIXES = (
        "API请求:", "API响应状态码:", "API响应内容:",
        "getFaceEncParams", "getEncParams",
        "AddDateBase",
    )
    _NOISE_SUBSTRINGS = ("appType", "signatureMD5", "access_token", "versionCode")

    def _is_key_line(line: str) -> bool:
        # strip timestamp prefix (HH:MM:SS ) before matching
        msg = line[9:] if len(line) > 9 else line
        for p in _NOISE_PREFIXES:
            if msg.startswith(p):
                return False
        for s in _NOISE_SUBSTRINGS:
            if s in msg:
                return False
        return True

    def _write_log(line: str):
        if _is_key_line(line):
            log_fh.write(line + "\n")

    async def _drain_queue():
        while True:
            await asyncio.sleep(3)
            while not queue.empty():
                try:
                    line = queue.get_nowait()
                    if line is not None:
                        _write_log(line)
                except Exception:
                    break

    async for db in get_db():
        stmt = select(Order).options(
            selectinload(Order.account).selectinload(TsnAccount_Model.school)
        ).where(Order.id == order_id)
        result = await db.execute(stmt)
        order = result.scalar_one_or_none()
        if order is None:
            log_fh.close()
            return

        order.status = "running"
        order.started_at = datetime.now()
        await db.commit()

        # 检查该学校是否有路线数据，没有则先爬取
        school_code = order.account.school.school_code if order.account and order.account.school else None
        if school_code:
            from sqlalchemy import func as sqlfunc
            path_count_stmt = select(sqlfunc.count(RunPath.id)).where(RunPath.school_code == school_code)
            path_count_result = await db.execute(path_count_stmt)
            path_count = path_count_result.scalar()
            if path_count == 0:
                logger.info(f"学校 {school_code} 无路线数据，开始自动爬取...")
                try:
                    await startSpider(order.account_id)
                    logger.info(f"学校 {school_code} 路线爬取完成")
                except Exception as spider_err:
                    logger.warning(f"路线爬取失败: {spider_err}，继续尝试跑步")

        drain_task = asyncio.create_task(_drain_queue())

        try:
            run_type = RUN_TYPE_MAP[order.run_type]
            run_server = TsnRunServer(
                accountId=order.account_id,
                runKiloMeter=order.distance,
                logRunType=run_type,
                useImageBed=bool(order.use_image_bed),
                pace=order.pace,
            )
            await run_server.startRunHandle()
            order.status = "completed"
            order.result_msg = f"跑步完成，距离 {order.distance} km"
        except Exception as e:
            logger.exception(e)
            order.status = "failed"
            order.error_msg = str(e)
            # 人脸401：重新认证后立刻重跑一次
            from TiShiNengError import TiShiNengError as _TsnErr
            is_face_401 = (isinstance(e, _TsnErr) and e.code == 401 and "人脸识别失败" in str(e))
            if is_face_401 and order.plan_id and "[401重试]" not in (order.error_msg or ""):
                order.error_msg = str(e) + " [将重新认证后重试]"
                _plan_id = order.plan_id
                _order_id = order.id
                async def _reauth_and_retry(oid: int, pid: int):
                    logger.info(f"人脸401：计划 #{pid} 订单 #{oid} 开始重新认证")
                    async for retry_db in get_db():
                        orig = (await retry_db.execute(select(Order).where(Order.id == oid))).scalar_one_or_none()
                        if orig is None:
                            return
                        acct = (await retry_db.execute(
                            select(TsnAccount_Model).options(selectinload(TsnAccount_Model.school))
                            .where(TsnAccount_Model.id == orig.account_id)
                        )).scalar_one_or_none()
                        if acct is None:
                            return
                        retry_plan = (await retry_db.execute(
                            select(Plan).where(Plan.id == pid)
                        )).scalar_one_or_none()
                        if retry_plan is None or retry_plan.status != "active":
                            return
                        try:
                            await tsnPasswordAuthServer(acct.school_id, acct.username, acct.password, retry_db)
                            logger.info(f"人脸401：账号 {acct.username} 重新认证成功，创建重跑订单")
                        except Exception as auth_e:
                            logger.warning(f"人脸401：账号 {acct.username} 重新认证失败: {auth_e}")
                            return
                        new_order = Order(
                            account_id=orig.account_id,
                            plan_id=pid,
                            run_type=orig.run_type,
                            distance=orig.distance,
                            status="pending",
                            created_at=datetime.now(),
                            use_image_bed=orig.use_image_bed,
                            pace=orig.pace,
                            username=orig.username,
                            school_name=orig.school_name,
                            error_msg="人脸401重试",
                        )
                        retry_db.add(new_order)
                        await retry_db.flush()
                        retry_plan.last_run_date = datetime.now().strftime("%Y-%m-%d")
                        await retry_db.commit()
                        asyncio.create_task(_execute_order(new_order.id))
                        logger.info(f"人脸401：已创建重跑订单 #{new_order.id}")
                asyncio.create_task(_reauth_and_retry(_order_id, _plan_id))
            # 网络繁忙：20分钟后自动重跑一次（通过查父订单 error_msg 判断是否已重试过）
            if "网络繁忙" in str(e) and order.plan_id:
                # 检查该计划今天是否已有过"网络繁忙重试"的订单，避免无限重试
                already_retried_stmt = select(Order).where(
                    Order.plan_id == order.plan_id,
                    Order.error_msg.like("%网络繁忙%[重试]%"),
                    Order.created_at >= datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d"),
                )
                already_retried_result = await db.execute(already_retried_stmt)
                if already_retried_result.scalar_one_or_none() is None:
                    order.error_msg = str(e) + " [将在20分钟后重试]"
                    _plan_id = order.plan_id
                    _order_id = order.id
                    async def _retry_after_delay(oid: int, pid: int):
                        await asyncio.sleep(20 * 60)
                        logger.info(f"网络繁忙重试：计划 #{pid} 订单 #{oid} 开始重跑")
                        async for retry_db in get_db():
                            retry_plan = (await retry_db.execute(
                                select(Plan).options(selectinload(Plan.account).selectinload(TsnAccount_Model.school))
                                .where(Plan.id == pid)
                            )).scalar_one_or_none()
                            if retry_plan is None or retry_plan.status != "active":
                                return
                            orig = (await retry_db.execute(select(Order).where(Order.id == oid))).scalar_one_or_none()
                            if orig is None:
                                return
                            new_order = Order(
                                account_id=orig.account_id,
                                plan_id=pid,
                                run_type=orig.run_type,
                                distance=orig.distance,
                                status="pending",
                                created_at=datetime.now(),
                                use_image_bed=orig.use_image_bed,
                                pace=orig.pace,
                                username=orig.username,
                                school_name=orig.school_name,
                                error_msg="网络繁忙重试",
                            )
                            retry_db.add(new_order)
                            await retry_db.flush()
                            retry_plan.last_run_date = datetime.now().strftime("%Y-%m-%d")
                            await retry_db.commit()
                            asyncio.create_task(_execute_order(new_order.id))
                            logger.info(f"网络繁忙重试：已创建重跑订单 #{new_order.id}")
                    asyncio.create_task(_retry_after_delay(_order_id, _plan_id))
        finally:
            drain_task.cancel()
            # 排空队列剩余内容写文件
            while not queue.empty():
                try:
                    line = queue.get_nowait()
                    if line is not None:
                        _write_log(line)
                except Exception:
                    break
            log_fh.close()
            order.completed_at = datetime.now()

            # 若属于计划任务，更新进度（防止重复计入：同一计划今天已有其他 completed 则跳过）
            if order.plan_id and order.status == "completed":
                today_str = datetime.now().strftime("%Y-%m-%d")
                dup_stmt = select(Order).where(
                    Order.plan_id == order.plan_id,
                    Order.status == "completed",
                    Order.id != order.id,
                    Order.completed_at >= datetime.strptime(today_str, "%Y-%m-%d"),
                )
                dup_result = await db.execute(dup_stmt)
                if dup_result.scalar_one_or_none() is not None:
                    logger.warning(f"计划 #{order.plan_id} 今天已有其他完成订单，跳过重复计入进度")
                else:
                    plan_stmt = select(Plan).where(Plan.id == order.plan_id)
                    plan_result = await db.execute(plan_stmt)
                    plan = plan_result.scalar_one_or_none()
                    if plan and plan.status == "active":
                        plan.completed_distance = round(plan.completed_distance + order.distance, 2)
                        plan.last_run_date = datetime.now().strftime("%Y-%m-%d")
                        if plan.completed_distance >= plan.total_distance:
                            plan.status = "completed"
                            logger.info(f"计划 #{plan.id} 已完成，总里程 {plan.total_distance} km")

            await db.commit()
            # 发送结束哨兵
            await queue.put(None)
            await asyncio.sleep(10)
            _order_log_queues.pop(order_id, None)


# ── 订单接口 ──────────────────────────────────────────────────────────────────

@app.get("/api/orders")
async def list_orders(current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        stmt = (
            select(Order)
            .options(selectinload(Order.account).selectinload(TsnAccount_Model.school))
            .order_by(Order.id.desc())
            .limit(50)
        )
        if not current_user.is_admin:
            stmt = stmt.join(TsnAccount_Model, Order.account_id == TsnAccount_Model.id).where(TsnAccount_Model.managed_by == current_user.username)
        result = await db.execute(stmt)
        orders = result.scalars().all()
        return [_order_dict(o) for o in orders]


@app.get("/api/orders/{order_id}")
async def get_order(order_id: int, current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        stmt = (
            select(Order)
            .options(selectinload(Order.account).selectinload(TsnAccount_Model.school))
            .where(Order.id == order_id)
        )
        result = await db.execute(stmt)
        order = result.scalar_one_or_none()
        if order is None:
            raise HTTPException(status_code=404, detail="订单不存在")
        if not current_user.is_admin and (order.account is None or order.account.managed_by != current_user.username):
            raise HTTPException(status_code=403, detail="无权访问此订单")
        return _order_dict(order)


@app.post("/api/orders/{order_id}/retry", status_code=201)
async def retry_order(order_id: int, current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        stmt = select(Order).options(selectinload(Order.account)).where(Order.id == order_id)
        result = await db.execute(stmt)
        order = result.scalar_one_or_none()
        if order is None:
            raise HTTPException(status_code=404, detail="订单不存在")
        if not current_user.is_admin and (order.account is None or order.account.managed_by != current_user.username):
            raise HTTPException(status_code=403, detail="无权操作此订单")
        if order.status not in ("failed", "completed"):
            raise HTTPException(status_code=400, detail="只有失败或已完成的订单才能重跑")

        new_order = Order(
            account_id=order.account_id,
            plan_id=order.plan_id,
            run_type=order.run_type,
            distance=order.distance,
            status="pending",
            created_at=datetime.now(),
            use_image_bed=order.use_image_bed,
            pace=order.pace,
            username=order.username,
            school_name=order.school_name,
        )

        # 若原订单没有 plan_id，查找该账号是否有活跃计划，有则关联
        if not order.plan_id:
            active_plan_stmt = select(Plan).where(
                Plan.account_id == order.account_id,
                Plan.status == "active",
                Plan.completed_distance < Plan.total_distance,
            ).order_by(Plan.id.asc()).limit(1)
            active_plan_result = await db.execute(active_plan_stmt)
            active_plan = active_plan_result.scalar_one_or_none()
            if active_plan:
                new_order.plan_id = active_plan.id
                # 占位防止调度器当天重复触发
                active_plan.last_run_date = datetime.now().strftime("%Y-%m-%d")
        db.add(new_order)
        await db.commit()
        await db.refresh(new_order)
        new_id = new_order.id

    asyncio.create_task(_execute_order(new_id))
    return {"id": new_id, "status": "pending"}


@app.get("/api/orders/{order_id}/logs")
async def get_order_logs(order_id: int, tail: int = 100, offset: int = 0, current_user: WebUser = Depends(get_current_user)):
    """返回订单日志。tail=0 返回全部；否则返回最后 tail 行（offset 用于翻页往前加载）"""
    if not current_user.is_admin:
        async for db in get_db():
            _o = (await db.execute(select(Order).options(selectinload(Order.account)).where(Order.id == order_id))).scalar_one_or_none()
            if _o is None:
                raise HTTPException(status_code=404, detail="订单不存在")
            if _o.account is None or _o.account.managed_by != current_user.username:
                raise HTTPException(status_code=403, detail="无权访问此订单")
    if log_file := (LOGS_DIR / f"{order_id}.log") if (LOGS_DIR / f"{order_id}.log").exists() else None:
        content = log_file.read_text(encoding="utf-8")
    else:
        async for db in get_db():
            stmt = select(Order).where(Order.id == order_id)
            result = await db.execute(stmt)
            order = result.scalar_one_or_none()
            if order is None:
                raise HTTPException(status_code=404, detail="订单不存在")
            content = order.logs or ""

    lines = content.splitlines()
    total = len(lines)
    if tail == 0:
        return {"lines": lines, "total": total, "has_more": False}
    # 从末尾往前取，offset 控制已加载多少行（用于"加载更多"）
    end = total - offset
    start = max(0, end - tail)
    return {"lines": lines[start:end], "total": total, "has_more": start > 0}


@app.get("/api/orders/{order_id}/stream")
async def stream_order_logs(order_id: int, current_user: WebUser = Depends(get_current_user)):
    """SSE：实时推送运行中订单的日志"""
    async def event_gen():
        queue = _order_log_queues.get(order_id)
        if queue is None:
            # 订单已结束，读文件一次性返回
            log_file = LOGS_DIR / f"{order_id}.log"
            if log_file.exists():
                content = log_file.read_text(encoding="utf-8")
            else:
                # 兼容旧数据从 DB 读
                content = ""
                async for db in get_db():
                    stmt = select(Order).where(Order.id == order_id)
                    result = await db.execute(stmt)
                    order = result.scalar_one_or_none()
                    if order and order.logs:
                        content = order.logs
            for line in content.splitlines():
                yield f"data: {line}\n\n"
            yield "data: [END]\n\n"
            return

        while True:
            try:
                msg = await asyncio.wait_for(queue.get(), timeout=25.0)
                if msg is None:
                    yield "data: [END]\n\n"
                    break
                yield f"data: {msg}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── 一键下单 ──────────────────────────────────────────────────────────────────

def _save_face_image(school_id: int, user_id: str, image_bytes: bytes, filename: str):
    face_dir = Path(__file__).parent / "face_images" / str(school_id) / str(user_id)
    face_dir.mkdir(parents=True, exist_ok=True)
    for old in list(face_dir.glob("*.jpg")) + list(face_dir.glob("*.png")):
        old.unlink()
    raw_ext = Path(filename).suffix.lower()
    ext = raw_ext if raw_ext in (".jpg", ".jpeg", ".png") else ".jpg"
    dest = face_dir / f"uploaded{ext}"
    dest.write_bytes(image_bytes)
    logger.info(f"人脸图片已覆盖保存: {dest}")


@app.post("/api/orders/place", status_code=201)
async def place_order(
    school_id: int = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    run_type: str = Form("sumRun"),
    distance: float = Form(2.0),
    total_distance: Optional[float] = Form(None),
    use_image_bed: bool = Form(False),
    pace: Optional[float] = Form(None),
    scheduled_hour: Optional[float] = Form(None),
    face_image: Optional[UploadFile] = File(None),
    current_user: WebUser = Depends(get_current_user),
):
    if run_type not in RUN_TYPE_MAP:
        raise HTTPException(status_code=422, detail=f"无效的跑步类型，可选: {', '.join(RUN_TYPE_MAP.keys())}")
    if distance <= 0 or distance > 50:
        raise HTTPException(status_code=422, detail="距离须在 0~50 km 之间")
    if scheduled_hour is not None and not (0 <= scheduled_hour < 24):
        raise HTTPException(status_code=422, detail="scheduled_hour 须在 0~23.99 之间")
    if pace is not None and not (3 <= pace <= 12):
        raise HTTPException(status_code=422, detail="配速须在 3~12 分钟/公里之间")
    if total_distance is not None:
        if total_distance > 500:
            raise HTTPException(status_code=422, detail="总距离须在 0~500 km 之间")
        if total_distance < distance:
            raise HTTPException(status_code=422, detail="总距离不能小于每次距离")
    is_plan = total_distance is not None and total_distance >= distance

    async for db in get_db():
        try:
            await tsnPasswordAuthServer(school_id, username, password, db)
        except Exception as e:
            logger.exception(e)
            msg = str(e) or type(e).__name__
            raise HTTPException(status_code=400, detail=f"授权失败: {msg}")

        stmt = (
            select(TsnAccount_Model)
            .options(selectinload(TsnAccount_Model.school))
            .where(TsnAccount_Model.username == username,
                   TsnAccount_Model.school_id == school_id)
        )
        result = await db.execute(stmt)
        account = result.scalar_one_or_none()
        if account is None:
            raise HTTPException(status_code=500, detail="账号授权后未找到记录")
        if not current_user.is_admin and account.managed_by is None:
            account.managed_by = current_user.username
            await db.flush()

        if face_image and face_image.filename:
            image_bytes = await face_image.read()
            if image_bytes:
                _save_face_image(account.school_id, account.user_id,
                                 image_bytes, face_image.filename)

        if is_plan:
            # 扣除渔货
            result_u = await db.execute(select(WebUser).where(WebUser.username == current_user.username))
            web_user_obj = result_u.scalar_one_or_none()
            have = web_user_obj.credits if web_user_obj else 0
            if have < total_distance:
                raise HTTPException(status_code=402, detail=f"渔货不足，需要 {total_distance} 渔货，当前余额 {have}")
            web_user_obj.credits = round(web_user_obj.credits - total_distance, 4)
            await db.flush()

            # 检查今天是否已有完成的订单，有则从明天开始
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            ran_stmt = select(Order).where(
                Order.account_id == account.id,
                Order.status == "completed",
                Order.completed_at >= today_start,
            )
            ran_result = await db.execute(ran_stmt)
            already_ran_today = ran_result.scalar_one_or_none() is not None

            plan = Plan(
                account_id=account.id,
                run_type=run_type,
                total_distance=total_distance,
                daily_limit=distance,
                use_image_bed=use_image_bed,
                pace=pace,
                scheduled_hour=scheduled_hour if scheduled_hour is not None else (datetime.now().hour + datetime.now().minute / 60),
                last_run_date=datetime.now().strftime("%Y-%m-%d") if already_ran_today else None,
            )
            db.add(plan)
            await db.commit()
            await db.refresh(plan)
            return {"id": plan.id, "type": "plan", "status": "active",
                    "message": f"计划已创建，共 {total_distance} km，每天最多 {distance} km，已扣除 {total_distance} 渔货"}

        order = Order(
            account_id=account.id,
            run_type=run_type,
            distance=distance,
            status="pending",
            created_at=datetime.now(),
            use_image_bed=use_image_bed,
            pace=pace,
            username=account.username,
            school_name=account.school.school_name if account.school else "",
        )
        db.add(order)
        await db.commit()
        await db.refresh(order)
        order_id = order.id

    asyncio.create_task(_execute_order(order_id))
    return {"id": order_id, "status": "pending"}


# ── 跑步记录查询 ──────────────────────────────────────────────────────────────

@app.get("/api/run-records")
async def get_run_records(username: str, school_id: int, date: str = "", password: str = "", current_user: WebUser = Depends(get_current_user)):
    if not current_user.is_admin:
        async for _db in get_db():
            _acc = (await _db.execute(select(TsnAccount_Model).where(TsnAccount_Model.username == username, TsnAccount_Model.school_id == school_id))).scalar_one_or_none()
            if _acc is None or _acc.managed_by != current_user.username:
                raise HTTPException(status_code=403, detail="无权查询此账号的跑步记录")
    from tsnClient import getTsnClientById
    from services.tsnSchool.tsnSchoolDao import getSchoolBySchoolId
    async for db in get_db():
        stmt = (
            select(TsnAccount_Model)
            .options(selectinload(TsnAccount_Model.school))
            .where(TsnAccount_Model.username == username,
                   TsnAccount_Model.school_id == school_id)
        )
        result = await db.execute(stmt)
        account = result.scalar_one_or_none()

        if account is not None:
            # 已授权账号，直接用库里的 token
            try:
                client = await getTsnClientById(account.id, db)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"获取授权失败: {e}")
            school = account.school
            is_public = school.sys_type == 2 if school else True
        elif password:
            # 未授权账号，临时登录，不写库
            school = await getSchoolBySchoolId(school_id, db)
            if school is None:
                raise HTTPException(status_code=404, detail="学校不存在")
            try:
                client = await _temp_login_client(school, username, password)
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"登录失败: {e}")
            is_public = school.sys_type == 2
        else:
            raise HTTPException(status_code=404, detail="该学号未在平台授权，请输入密码查询")

        try:
            if is_public:
                raw = await client.listExerciseRecord(runStatus=1, date=date)
                records = _fmt_public_records(raw)
                dates = raw.get("dates", []) if isinstance(raw, dict) else []
            else:
                raw = await client.appSportRecordList(pageSize=20)
                records = _fmt_private_records(raw)
                dates = []
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"查询失败: {e}")
        finally:
            try:
                await client.close()
            except Exception:
                pass

        school_name = school.school_name if school else ""
        return {
            "username": username,
            "school_name": school_name,
            "records": records,
            "dates": [{"date": d.get("date", ""), "count": d.get("count", 0), "total_range": d.get("totalRange", 0)} for d in dates],
        }


async def _temp_login_client(school, username: str, password: str):
    """临时登录，返回 SDK 客户端，不写数据库"""
    import uuid as _uuid
    from TiShiNengSdkPublic import TiShiNengSdkPublic
    from TiShiNengSdkPrivate import TiShiNengPrivate
    from TiShiNengError import TiShiNengError

    device_id = str(_uuid.uuid4())
    if school.sys_type == 2:
        tsn = TiShiNengSdkPublic(0, school.school_id, school.school_code, school.open_id,
                                  device_id, deviceModel.brand, deviceModel.model, deviceModel.osver,
                                  "", deviceModel.a_list)
        if school.lan_url:
            tsn.setCloudUrl(school.lan_url)
        login = await tsn.getAccessToken(username, password)
        if "msg" in login:
            msg = login["msg"]
            if "Bad credentials" in msg:
                raise TiShiNengError("用户名错误")
            if "Wrong password" in msg:
                raise TiShiNengError("密码错误")
            raise TiShiNengError(msg)
        uid = login["user_id"]
        token = login["access_token"]
        tsn2 = TiShiNengSdkPublic(uid, school.school_id, school.school_code, school.open_id,
                                   device_id, deviceModel.brand, deviceModel.model, deviceModel.osver,
                                   token, deviceModel.a_list)
        if school.lan_url:
            tsn2.setCloudUrl(school.lan_url)
        return tsn2
    else:
        tsn = TiShiNengPrivate(0, school.school_id, school.school_code, school.is_open_encry,
                               device_id, deviceModel.brand, deviceModel.model, "")
        tsn.setAppId(school.open_id)
        tsn.setSchoolUrl(school.school_url)
        login = await tsn.appLogin(username, password)
        uid = login["id"]
        token = login["token"]
        tsn2 = TiShiNengPrivate(uid, school.school_id, school.school_code, school.is_open_encry,
                                device_id, deviceModel.brand, deviceModel.model, token)
        tsn2.setAppId(school.open_id)
        tsn2.setSchoolUrl(school.school_url)
        return tsn2


def _fmt_public_records(raw) -> list:
    if not raw or not isinstance(raw, dict):
        return []
    items = raw.get("records") or []
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        # startTime 是毫秒时间戳
        start_ms = item.get("startTime")
        try:
            date_str = datetime.fromtimestamp(int(start_ms) / 1000).strftime("%Y-%m-%d %H:%M")
        except Exception:
            date_str = str(start_ms or "—")
        # sportTime 是秒数
        sport_sec = item.get("sportTime")
        try:
            sec = int(float(sport_sec))
            duration_str = f"{sec // 60}分{sec % 60:02d}秒"
        except Exception:
            duration_str = str(sport_sec or "—")
        status_val = str(item.get("status", ""))
        status_str = "合格" if status_val == "1" else ("不合格" if status_val == "0" else status_val)
        result.append({
            "record_id": str(item.get("id") or ""),
            "date": date_str,
            "distance": item.get("sportRange") or item.get("reachRange") or 0,
            "duration": duration_str,
            "pace": item.get("speed") or "—",
            "status": status_str,
            "run_type": item.get("sportTypeName") or "—",
        })
    return result


def _fmt_private_records(raw) -> list:
    if not raw or not isinstance(raw, (list, dict)):
        return []
    items = raw if isinstance(raw, list) else raw.get("list") or raw.get("records") or raw.get("data") or []
    if isinstance(items, dict):
        items = items.get("list") or items.get("records") or items.get("data") or []
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        result.append({
            "record_id": str(item.get("sportRecordId") or item.get("id") or ""),
            "date": str(item.get("createTime") or item.get("startTime") or item.get("date") or "—")[:10],
            "distance": item.get("formatSportRange") or item.get("sportRange") or item.get("distance") or 0,
            "duration": item.get("formatSportTime") or item.get("sportTime") or "—",
            "pace": item.get("pace") or item.get("avgSpeed") or "—",
            "status": str(item.get("qualifiedStatus") or item.get("status") or "—"),
            "run_type": item.get("runTypeName") or item.get("sportTypeName") or "—",
        })
    return result


# ── 计划任务 ──────────────────────────────────────────────────────────────────

class PlanCreate(BaseModel):
    account_id: int
    run_type: str
    total_distance: float
    daily_limit: float


def _plan_dict(plan: Plan) -> dict:
    remaining = max(0.0, round(plan.total_distance - plan.completed_distance, 2))
    return {
        "id": plan.id,
        "account_id": plan.account_id,
        "username": plan.account.username if plan.account else "",
        "password": plan.account.password if plan.account else "",
        "school_name": (plan.account.school.school_name if plan.account and plan.account.school else ""),
        "run_type": plan.run_type,
        "run_type_label": RUN_TYPE_LABELS.get(plan.run_type, plan.run_type),
        "total_distance": plan.total_distance,
        "daily_limit": plan.daily_limit,
        "completed_distance": plan.completed_distance,
        "remaining_distance": remaining,
        "status": plan.status,
        "last_run_date": plan.last_run_date,
        "scheduled_hour": plan.scheduled_hour,
        "created_at": _fmt_dt(plan.created_at),
    }


@app.get("/api/plans")
async def list_plans(current_user: WebUser = Depends(get_current_user)):
    from sqlalchemy import func as sqlfunc
    async for db in get_db():
        stmt = (
            select(Plan)
            .options(selectinload(Plan.account).selectinload(TsnAccount_Model.school))
            .order_by(Plan.id.desc())
        )
        if not current_user.is_admin:
            stmt = stmt.join(TsnAccount_Model, Plan.account_id == TsnAccount_Model.id).where(TsnAccount_Model.managed_by == current_user.username)
        result = await db.execute(stmt)
        plans = result.scalars().all()

        # 一次查出每个计划今天最新的订单
        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        plan_ids = [p.id for p in plans]
        today_orders: dict[int, Order] = {}
        if plan_ids:
            # 取每个 plan_id 今天 created_at 最大的那条（含 running/pending）
            subq = (
                select(Order.plan_id, sqlfunc.max(Order.id).label("max_id"))
                .where(Order.plan_id.in_(plan_ids), Order.created_at >= today_start)
                .group_by(Order.plan_id)
                .subquery()
            )
            today_stmt = select(Order).join(subq, Order.id == subq.c.max_id)
            today_result = await db.execute(today_stmt)
            for o in today_result.scalars().all():
                today_orders[o.plan_id] = o

        # 补充：把内存中正在运行的订单也纳入（防止 created_at 时区差导致漏查）
        running_order_ids = set(_order_log_queues.keys())
        if running_order_ids:
            running_stmt = (
                select(Order)
                .where(Order.id.in_(running_order_ids), Order.plan_id.in_(plan_ids))
            )
            running_result = await db.execute(running_stmt)
            for o in running_result.scalars().all():
                if o.plan_id is not None:
                    existing = today_orders.get(o.plan_id)
                    # 优先用 id 更大（更新）的，但 running 状态优先覆盖 failed
                    if existing is None or o.status in ("pending", "running"):
                        today_orders[o.plan_id] = o

        today = datetime.now().strftime("%Y-%m-%d")
        out = []
        for p in plans:
            d = _plan_dict(p)
            today_order = today_orders.get(p.id)
            # 有今天的订单（含 running/pending）直接以订单状态为准，不受 last_run_date 影响
            if today_order is not None:
                d["today_status"] = _infer_day_status(today_order)
                d["today_order_id"] = today_order.id
            elif p.last_run_date != today:
                d["today_status"] = "pending"
                d["today_order_id"] = None
            else:
                # last_run_date == today 但查不到订单（极少情况）
                d["today_status"] = "pending"
                d["today_order_id"] = None
            out.append(d)
        return out


@app.post("/api/plans", status_code=201)
async def create_plan(body: PlanCreate, current_user: WebUser = Depends(get_current_user)):
    if body.run_type not in RUN_TYPE_MAP:
        raise HTTPException(status_code=422, detail="无效的跑步类型")
    if body.total_distance <= 0 or body.total_distance > 500:
        raise HTTPException(status_code=422, detail="总距离须在 0~500 km 之间")
    if body.daily_limit <= 0 or body.daily_limit > body.total_distance:
        raise HTTPException(status_code=422, detail="每日限额须大于 0 且不超过总距离")

    async for db in get_db():
        stmt = select(TsnAccount_Model).where(TsnAccount_Model.id == body.account_id)
        result = await db.execute(stmt)
        account = result.scalar_one_or_none()
        if account is None:
            raise HTTPException(status_code=404, detail="账号不存在")
        if not current_user.is_admin and account.managed_by != current_user.username:
            raise HTTPException(status_code=403, detail="只能为自己的账号创建计划")

        if not current_user.is_admin:
            result2 = await db.execute(select(WebUser).where(WebUser.username == current_user.username))
            web_user = result2.scalar_one_or_none()
            if web_user is None or web_user.credits < body.total_distance:
                have = web_user.credits if web_user else 0
                raise HTTPException(status_code=402, detail=f"渔货不足，需要 {body.total_distance} 渔货，当前余额 {have}")
            web_user.credits = round(web_user.credits - body.total_distance, 4)
            await db.flush()

        plan = Plan(
            account_id=body.account_id,
            run_type=body.run_type,
            total_distance=body.total_distance,
            daily_limit=body.daily_limit,
        )
        db.add(plan)
        await db.commit()
        await db.refresh(plan)
        return {"id": plan.id, "status": "active"}


@app.delete("/api/plans/{plan_id}", status_code=200)
async def cancel_plan(plan_id: int, current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        stmt = select(Plan).options(selectinload(Plan.account)).where(Plan.id == plan_id)
        result = await db.execute(stmt)
        plan = result.scalar_one_or_none()
        if plan is None:
            raise HTTPException(status_code=404, detail="计划不存在")
        if not current_user.is_admin and (plan.account is None or plan.account.managed_by != current_user.username):
            raise HTTPException(status_code=403, detail="无权操作此计划")
        if plan.status != "active":
            raise HTTPException(status_code=400, detail="只有进行中的计划才能取消")

        # 退还未跑部分的渔货
        refund = round(plan.total_distance - plan.completed_distance, 4)
        if refund > 0 and not current_user.is_admin:
            result_u = await db.execute(select(WebUser).where(WebUser.username == current_user.username))
            web_user = result_u.scalar_one_or_none()
            if web_user:
                web_user.credits = round(web_user.credits + refund, 4)

        plan.status = "cancelled"
        await db.commit()
        return {"id": plan_id, "status": "cancelled", "refund": refund}


@app.post("/api/plans/{plan_id}/run-now", status_code=200)
async def run_plan_now(plan_id: int, current_user: WebUser = Depends(get_current_user)):
    """立即为指定计划触发一次跑步，无视任何限制"""
    async for db in get_db():
        stmt = (
            select(Plan)
            .options(selectinload(Plan.account).selectinload(TsnAccount_Model.school))
            .where(Plan.id == plan_id)
        )
        result = await db.execute(stmt)
        plan = result.scalar_one_or_none()
        if plan is None:
            raise HTTPException(status_code=404, detail="计划不存在")
        if not current_user.is_admin and (plan.account is None or plan.account.managed_by != current_user.username):
            raise HTTPException(status_code=403, detail="无权操作此计划")
        if plan.status != "active":
            raise HTTPException(status_code=400, detail="只有进行中的计划才能重跑")

        # 剩余距离不足时用 daily_limit，保证始终能跑
        remaining = round(plan.total_distance - plan.completed_distance, 2)
        dist = round(min(plan.daily_limit, remaining) if remaining > 0 else plan.daily_limit, 2)

        order = Order(
            account_id=plan.account_id,
            plan_id=plan.id,
            run_type=plan.run_type,
            distance=dist,
            status="pending",
            created_at=datetime.now(),
            use_image_bed=plan.use_image_bed,
            pace=plan.pace,
            username=plan.account.username if plan.account else "",
            school_name=plan.account.school.school_name if plan.account and plan.account.school else "",
        )
        db.add(order)
        await db.flush()
        plan.last_run_date = datetime.now().strftime("%Y-%m-%d")
        await db.commit()
        asyncio.create_task(_execute_order(order.id))
        logger.info(f"run-now: 计划 #{plan.id} 立即触发订单 #{order.id}")
        return {"id": order.id, "plan_id": plan_id}


def _infer_day_status(order: Order) -> str:
    """从订单状态和 error_msg 推断今日状态标签"""
    if order.status in ("pending", "running"):
        return "running"
    if order.status == "completed":
        return "success"
    if order.status == "failed":
        msg = (order.error_msg or "").lower()
        if "每周" in msg or "本周" in msg or "week" in msg:
            return "week_limit"
        if "每日" in msg or "今日" in msg or "已达标" in msg:
            return "day_limit"
        if ("人脸" in msg or "face" in msg or
                "20001" in msg or "20002" in msg or "20003" in msg):
            return "face_error"
        if ("20010" in msg or "20011" in msg or "图床" in msg):
            return "bed_error"
        if "作弊" in msg or "黑名单" in msg or "禁跑" in msg:
            return "cheating"
        return "other"
    return "other"


@app.post("/api/plans/run-all", status_code=200)
async def run_all_plans(current_user: WebUser = Depends(get_current_user)):
    """对所有进行中的计划，今天还没跑的立即触发一条订单（忽略 scheduled_hour）"""
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="仅管理员可操作")
    async for db in get_db():
        today = datetime.now().strftime("%Y-%m-%d")
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        plans_stmt = (
            select(Plan)
            .options(selectinload(Plan.account).selectinload(TsnAccount_Model.school))
            .where(
                Plan.status == "active",
                Plan.completed_distance < Plan.total_distance,
                (Plan.last_run_date == None) | (Plan.last_run_date != today),
            )
        )
        plans_result = await db.execute(plans_stmt)
        plans = plans_result.scalars().all()

        triggered = 0
        skipped = 0
        for plan in plans:
            # 今天已有 pending/running/completed 订单则跳过
            existing_stmt = select(Order).where(
                Order.plan_id == plan.id,
                Order.created_at >= today_start,
                Order.status.in_(["pending", "running", "completed"]),
            )
            existing_result = await db.execute(existing_stmt)
            if existing_result.scalar_one_or_none() is not None:
                skipped += 1
                continue

            dist = round(min(plan.daily_limit, plan.total_distance - plan.completed_distance), 2)
            order = Order(
                account_id=plan.account_id,
                plan_id=plan.id,
                run_type=plan.run_type,
                distance=dist,
                status="pending",
                created_at=datetime.now(),
                use_image_bed=plan.use_image_bed,
                pace=plan.pace,
                username=plan.account.username if plan.account else "",
                school_name=plan.account.school.school_name if plan.account and plan.account.school else "",
            )
            db.add(order)
            await db.flush()
            plan.last_run_date = today
            await db.commit()
            # 随机错开启动时间，避免并发订单时间戳完全相同
            delay = triggered * 15 + random.uniform(0, 5)
            asyncio.create_task(_execute_order(order.id, start_delay=delay))
            logger.info(f"run-all: 计划 #{plan.id} 触发订单 #{order.id}，延迟 {delay:.1f}s")
            triggered += 1

        return {"triggered": triggered, "skipped": skipped}


@app.post("/api/plans/retry-failed", status_code=200)
async def retry_failed_plans(current_user: WebUser = Depends(get_current_user)):
    """对今天所有 face_error / bed_error / other 失败的计划子订单，各重跑一次（每个计划只重跑最近一条）"""
    async for db in get_db():
        today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        from sqlalchemy import func as sqlfunc
        subq = (
            select(Order.plan_id, sqlfunc.max(Order.id).label("max_id"))
            .where(
                Order.plan_id != None,
                Order.status == "failed",
                Order.created_at >= today_start,
            )
            .group_by(Order.plan_id)
            .subquery()
        )
        failed_stmt = (
            select(Order)
            .options(selectinload(Order.account).selectinload(TsnAccount_Model.school))
            .join(subq, Order.id == subq.c.max_id)
        )
        failed_result = await db.execute(failed_stmt)
        failed_orders = failed_result.scalars().all()

        retriggered = 0
        for old_order in failed_orders:
            # 只重跑 face_error、bed_error 和 other 类型
            day_status = _infer_day_status(old_order)
            if day_status not in ("face_error", "bed_error", "other"):
                continue

            # 非管理员只能重跑自己管辖的账号
            if not current_user.is_admin:
                acct = old_order.account
                if acct is None or acct.managed_by != current_user.username:
                    continue

            # 确认计划仍然活跃
            plan_stmt = select(Plan).where(Plan.id == old_order.plan_id, Plan.status == "active")
            plan_result = await db.execute(plan_stmt)
            plan = plan_result.scalar_one_or_none()
            if plan is None:
                continue

            # 跳过已有 pending/running 订单的计划
            active_stmt = select(Order).where(
                Order.plan_id == old_order.plan_id,
                Order.status.in_(["pending", "running"]),
            )
            active_result = await db.execute(active_stmt)
            if active_result.scalar_one_or_none() is not None:
                continue

            new_order = Order(
                account_id=old_order.account_id,
                plan_id=old_order.plan_id,
                run_type=old_order.run_type,
                distance=old_order.distance,
                status="pending",
                created_at=datetime.now(),
                use_image_bed=old_order.use_image_bed,
                pace=old_order.pace,
                username=old_order.username,
                school_name=old_order.school_name,
            )
            db.add(new_order)
            await db.flush()
            plan.last_run_date = datetime.now().strftime("%Y-%m-%d")
            await db.commit()
            delay = retriggered * 15 + random.uniform(0, 5)
            asyncio.create_task(_execute_order(new_order.id, start_delay=delay))
            logger.info(f"retry-failed: 计划 #{plan.id} 重跑订单 #{new_order.id}，延迟 {delay:.1f}s")
            retriggered += 1

        return {"retriggered": retriggered}


@app.get("/api/plans/{plan_id}/orders")
async def list_plan_orders(plan_id: int, current_user: WebUser = Depends(get_current_user)):
    async for db in get_db():
        plan_stmt = select(Plan).options(selectinload(Plan.account)).where(Plan.id == plan_id)
        plan_result = await db.execute(plan_stmt)
        plan = plan_result.scalar_one_or_none()
        if plan is None:
            raise HTTPException(status_code=404, detail="计划不存在")
        if not current_user.is_admin and (plan.account is None or plan.account.managed_by != current_user.username):
            raise HTTPException(status_code=403, detail="无权访问此计划")

        stmt = (
            select(Order)
            .where(Order.plan_id == plan_id)
            .order_by(Order.id.desc())
        )
        result = await db.execute(stmt)
        orders = result.scalars().all()
        return [
            {
                "id": o.id,
                "date": _fmt_dt(o.created_at),
                "completed_at": _fmt_dt(o.completed_at),
                "distance": o.distance,
                "run_type_label": RUN_TYPE_LABELS.get(o.run_type, o.run_type),
                "status": o.status,
                "day_status": _infer_day_status(o),
                "error_msg": o.error_msg or "",
                "result_msg": o.result_msg or "",
            }
            for o in orders
        ]


# ── 计划调度器 ────────────────────────────────────────────────────────────────

def _calc_scheduled_time(hour: float) -> str:
    """将 hour（如 9.0=9点, 9.5=9:30）加上 ±20 分钟随机偏移，返回 'HH:MM' 字符串"""
    import random as _random
    base_minutes = int(hour * 60)
    offset = _random.randint(-20, 20)
    total = base_minutes + offset
    total = max(0, min(23 * 60 + 59, total))
    return f"{total // 60:02d}:{total % 60:02d}"


async def _plan_scheduler():
    """每分钟检查一次，触发到达计划时间的子订单"""
    await asyncio.sleep(30)  # 启动后稍等，让 DB 初始化完成
    while True:
        try:
            await _trigger_plan_orders()
        except Exception as e:
            logger.warning(f"计划调度器异常: {e}")
        await asyncio.sleep(60)


async def _trigger_plan_orders():
    now = datetime.now()
    today = now.strftime("%Y-%m-%d")
    current_hm = now.strftime("%H:%M")

    async for db in get_db():
        # 找今天还没跑的活跃计划
        plans_stmt = (
            select(Plan)
            .options(selectinload(Plan.account).selectinload(TsnAccount_Model.school))
            .where(
                Plan.status == "active",
                Plan.completed_distance < Plan.total_distance,
                (Plan.last_run_date == None) | (Plan.last_run_date != today),
            )
            .order_by(Plan.id.asc())
        )
        plans_result = await db.execute(plans_stmt)
        plans = plans_result.scalars().all()

        triggered = 0
        for plan in plans:
            # 有计划时间的计划：检查当前时间是否在 [scheduled_hour ± 20min] 窗口内
            if plan.scheduled_hour is not None:
                base_min = int(plan.scheduled_hour * 60)
                now_min = now.hour * 60 + now.minute
                if not (base_min - 20 <= now_min <= base_min + 20):
                    continue

            # 周上限检查：最近一条本周内的失败订单是周上限，则跳到下周一
            week_start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=now.weekday())
            week_limit_stmt = select(Order).where(
                Order.plan_id == plan.id,
                Order.status == "failed",
            ).order_by(Order.id.desc()).limit(1)
            week_limit_result = await db.execute(week_limit_stmt)
            last_failed = week_limit_result.scalar_one_or_none()
            if last_failed and last_failed.created_at >= week_start:
                err = (last_failed.error_msg or "").lower()
                if "每周" in err or "本周" in err or "week" in err:
                    # 下周一 = 今天 + (7 - weekday()) 天，weekday()=0时加7天
                    days_to_monday = 7 - now.weekday() if now.weekday() > 0 else 7
                    next_monday = (now + timedelta(days=days_to_monday)).strftime("%Y-%m-%d")
                    if plan.last_run_date != next_monday:
                        plan.last_run_date = next_monday
                        await db.commit()
                    continue

            # 额外检查：该账号今天是否已有成功的订单（含手动下单）
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            ran_stmt = select(Order).where(
                Order.account_id == plan.account_id,
                Order.status == "completed",
                Order.completed_at >= today_start,
            )
            ran_result = await db.execute(ran_stmt)
            if ran_result.scalar_one_or_none() is not None:
                plan.last_run_date = today
                await db.commit()
                continue

            dist = round(min(plan.daily_limit, plan.total_distance - plan.completed_distance), 2)
            order = Order(
                account_id=plan.account_id,
                plan_id=plan.id,
                run_type=plan.run_type,
                distance=dist,
                status="pending",
                created_at=datetime.now(),
                use_image_bed=plan.use_image_bed,
                pace=plan.pace,
                username=plan.account.username if plan.account else "",
                school_name=plan.account.school.school_name if plan.account and plan.account.school else "",
            )
            db.add(order)
            await db.flush()
            order_id = order.id
            plan.last_run_date = today
            await db.commit()
            delay = triggered * 15 + random.uniform(0, 5)
            asyncio.create_task(_execute_order(order_id, start_delay=delay))
            logger.info(f"计划 #{plan.id} 触发子订单 #{order_id}，距离 {dist} km，延迟 {delay:.1f}s")
            triggered += 1


# ── 管理员接口 ────────────────────────────────────────────────────────────────

@app.get("/api/admin/users")
async def admin_list_users(current_user: WebUser = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="仅管理员可操作")
    async for db in get_db():
        from sqlalchemy import func as sqlfunc
        users_result = await db.execute(select(WebUser).order_by(WebUser.id))
        users = users_result.scalars().all()

        # 统计每个 managed_by 账号下所有计划已跑总公里
        km_result = await db.execute(
            select(TsnAccount_Model.managed_by, sqlfunc.sum(Plan.completed_distance))
            .join(Plan, Plan.account_id == TsnAccount_Model.id)
            .group_by(TsnAccount_Model.managed_by)
        )
        km_map = {row[0]: round(row[1], 2) for row in km_result.all() if row[0]}

        # 统计每个 managed_by 下总下单公里（plans total_distance 之和）
        total_result = await db.execute(
            select(TsnAccount_Model.managed_by, sqlfunc.sum(Plan.total_distance))
            .join(Plan, Plan.account_id == TsnAccount_Model.id)
            .group_by(TsnAccount_Model.managed_by)
        )
        total_map = {row[0]: round(row[1], 2) for row in total_result.all() if row[0]}

        return [
            {
                "id": u.id,
                "username": u.username,
                "is_admin": u.is_admin,
                "credits": u.credits,
                "completed_km": km_map.get(u.username, 0),
                "ordered_km": total_map.get(u.username, 0),
            }
            for u in users
        ]


class CreditsAdjust(BaseModel):
    user_id: int
    amount: float


class WebUserCreate(BaseModel):
    username: str
    password: str
    credits: float = 0.0


@app.post("/api/admin/users")
async def admin_create_user(body: WebUserCreate, current_user: WebUser = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="仅管理员可操作")
    if not body.username or not body.password:
        raise HTTPException(status_code=400, detail="用户名和密码不能为空")
    async for db in get_db():
        existing = await db.execute(select(WebUser).where(WebUser.username == body.username))
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="用户名已存在")
        new_user = WebUser(
            username=body.username,
            hashed_password=_hash_password(body.password),
            is_admin=False,
            credits=body.credits,
        )
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        return {"id": new_user.id, "username": new_user.username, "credits": new_user.credits}


@app.delete("/api/admin/users/{user_id}")
async def admin_delete_user(user_id: int, current_user: WebUser = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="仅管理员可操作")
    async for db in get_db():
        result = await db.execute(select(WebUser).where(WebUser.id == user_id))
        user = result.scalar_one_or_none()
        if user is None:
            raise HTTPException(status_code=404, detail="用户不存在")
        if user.is_admin:
            raise HTTPException(status_code=400, detail="不能删除管理员账号")
        await db.delete(user)
        await db.commit()
        return {"ok": True}


@app.post("/api/admin/credits")
async def admin_adjust_credits(body: CreditsAdjust, current_user: WebUser = Depends(get_current_user)):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="仅管理员可操作")
    async for db in get_db():
        result = await db.execute(select(WebUser).where(WebUser.id == body.user_id))
        user = result.scalar_one_or_none()
        if user is None:
            raise HTTPException(status_code=404, detail="用户不存在")
        user.credits = round(user.credits + body.amount, 4)
        await db.commit()
        return {"username": user.username, "credits": user.credits}
