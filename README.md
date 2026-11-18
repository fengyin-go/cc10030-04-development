# 数据湖入湖治理中心

面向数据湖的多区数据入湖、格式管理、分层治理和生命周期维护的数据湖建设平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 湖区分层 | `lake_zone` | 湖区定义 | 湖区编码、湖区名称、分层级别 |
| 入湖管道 | `ingest_pipeline` | 管道配置 | 管道名称、源端类型、目标湖区 |
| Schema注册 | `schema_registry` | Schema版本 | 主题名称、Schema定义、版本号 |
| 分区策略 | `partition_policy` | 分区规则 | 规则名称、目标数据集、分区键 |
| 分区合并计划 | `partition_merge_plan` | 分区合并计划 | 计划编号、快照版本、合并分组数、计划版本 |
| 存储格式 | `file_format` | 格式模板 | 模板名称、文件格式、压缩算法 |
| 留存策略 | `data_retention` | 留存规则 | 规则名称、目标湖区、保留天数 |
| 元数据目录 | `metadata_catalog` | 元数据条目 | 条目ID、数据集名称、所属湖区 |
| 质量门禁 | `quality_gate` | 门禁规则 | 规则名称、检查维度、阈值条件 |
| 湖内去重 | `dedup_lake` | 去重配置 | 配置名称、目标数据集、去重键 |
| 变更追踪 | `lineage_track` | 变更记录 | 变更ID、目标对象、变更类型 |
| 操作审计 | `audit_journal` | 审计日志 | 操作时间、操作主体、操作类型 |
| 快照管理 | `snapshot_mgr` | 快照任务 | 快照名称、目标数据集、快照时间 |
| 过期清理 | `ttl_sweeper` | 清理策略 | 策略名称、清理范围、匹配条件 |
| 冷热分层 | `cold_warm_tier` | 分层策略 | 策略名称、热度评估、冷热阈值 |
| 跨区复制 | `cross_zone_copy` | 复制任务 | 任务名称、源湖区、目标湖区 |
| 模式演进 | `schema_evolve` | 演进方案 | 方案名称、目标Schema、变更类型 |
| 入湖监控 | `ingest_monitor` | 监控面板 | 监控项、数据管道、吞吐速率 |
| 灾备策略 | `backup_policy` | 灾备计划 | 计划名称、保护范围、备份周期 |
| 数据字典 | `data_dictionary` | 字典条目 | 条目编码、字段名称、数据类型 |
| 访问管控 | `access_control` | 访问策略 | 策略名称、资源范围、主体类型 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
