"""业务路由汇总：固定阶段路由先注册，通用模块资源路由兜底。"""
from __future__ import annotations

from app.routers import partition_merge, resource

# partition_merge 含 /api/partition-merge/* 固定路径，必须排在
# /api/{module} 动态资源路由之前，避免被通配模块名抢先匹配。
ROUTERS = [partition_merge, resource]
