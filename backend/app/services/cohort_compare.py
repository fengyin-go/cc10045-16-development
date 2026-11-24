"""分群对比领域服务：执行两个分群在多个维度上的统计差异对比。

真实数据流：指定分群A/分群B和对比维度
→ pipeline.compute_cohort_comparison 从 segment_members 读取成员属性
→ 计算均值差异/标准差/t-test显著性 → 写入 cohort_comparisons。

受控统计入口：面向协作部门按"成员归属 + 授权"两个维度发布对比统计。
分群引擎负责把授权成员和归属关联起来并提供实际纳入属性计算的对比数据；
本服务负责发布控制：业务范围校验、最少授权成员门槛、同期反推防护、
快照重开和拥有方审计。接收方只能拿到聚合结果，看不到个人成员明细。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import compute_cohort_comparison, compute_controlled_average
from app.services.base import BaseModuleService

# 每个发布的统计至少纳入的授权成员数（小样本细组不可单独可见）
MIN_AUTHORIZED = 3
# 拥有方身份标识：只有拥有方可以核查对比输出纳入了哪些授权样本
OWNER = "拥有方"


class CohortCompareService(BaseModuleService):
    MODULE = "cohort_compare"
    ENTITY = "对比报告"
    OPERATION_TERM = "对比"
    LIST_FIELDS = ["对比名称", "分群A", "分群B", "对比维度", "差异显著性"]
    REQUIRED_FIELDS = ["对比名称", "分群A", "分群B", "对比维度"]
    STATUS_ORDER = ["待计算", "计算中", "已完成", "计算失败"]
    ACTION_RULES = {"启动对比": "已完成", "取消对比": "计算失败"}
    NEGATIVE_ACTIONS = []

    def run_comparison(self, entry_id: int) -> dict[str, Any]:
        """执行分群对比。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            db_entry = db.fetchone("SELECT * FROM cohort_comparisons WHERE id=?", [entry_id])
            if db_entry is None:
                return {"ok": False, "message": f"对比记录 {entry_id} 不存在"}
            entry = dict(db_entry)

        db.update("cohort_comparisons", {"status": "计算中"}, "id=?", [entry_id])

        result = compute_cohort_comparison(entry)

        db.update("cohort_comparisons", {
            "status": "已完成",
            "result_summary": result["result_summary"],
        }, "id=?", [entry_id])

        return {
            "ok": True,
            "comparison_name": result["comparison_name"],
            "significant_count": result["significant_count"],
            "dimensions": result["dimension_results"],
        }

    def comparison_result(self, entry_id: int) -> dict[str, Any] | None:
        """获取对比结果。"""
        entry = db.fetchone("SELECT * FROM cohort_comparisons WHERE id=?", [entry_id])
        if entry is None:
            return None
        result = {"status": entry["status"]}
        if entry["result_summary"]:
            try:
                result["summary"] = json.loads(entry["result_summary"])
            except json.JSONDecodeError:
                result["summary"] = entry["result_summary"]
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_comparisons"] = db.count("cohort_comparisons")
        snapshot["completed"] = db.count("cohort_comparisons", "status='已完成'")
        return snapshot

    # ------------------------------------------------------------------
    # 受控统计入口：按成员归属 + 授权维度向协作部门发布对比统计
    # ------------------------------------------------------------------

    def request_controlled_stat(
        self,
        recipient: str,
        period: str,
        segment_ids: list[int],
        attr_key: str,
    ) -> dict[str, Any]:
        """接收方领取某发布期的受控统计（均值）。

        发布控制顺序：
          0. 本期同一份已发布 → 重开快照，数值不变；
          1. 业务范围：请求的归属必须落在接收方覆盖范围内；
          2. 分群引擎提供实际纳入属性计算的对比数据（仅授权成员）；
          3. 每个发布的统计至少三名授权成员，越权成员不能补足样本；
          4. 反推防护：与本期已交付报告叠加后不得暴露小样本细组；
          5. 发布并留存审计样本（仅拥有方可查）。
        交付内容只有聚合结果，不含任何个人成员明细。
        """
        from app.services import service_for  # 延迟导入，避免服务注册表循环依赖

        engine = service_for("segment_engine")
        seg_ids = sorted({int(s) for s in segment_ids})
        if not recipient or not period or not seg_ids or not attr_key:
            return {"ok": False, "delivered": False,
                    "reason": "缺少必要参数：接收方、发布期、归属分群、统计属性"}

        # 0) 本期同一份已发布：重开快照，数值不变
        snapshot = self._find_release(recipient, period, attr_key, seg_ids)
        if snapshot is not None:
            return {
                "ok": True, "delivered": True, "snapshot": True,
                "recipient": recipient, "period": period,
                "segment_ids": seg_ids, "attr_key": attr_key,
                "authorized_count": snapshot["authorized_count"],
                "stat_value": snapshot["stat_value"],
            }

        # 1) 业务范围：接收方只能领取覆盖其业务范围的结果
        scope = engine.recipient_scope(recipient)
        if not set(seg_ids) <= scope:
            return {"ok": False, "delivered": False,
                    "reason": "超出业务范围：请求的归属不在接收方覆盖范围内",
                    "segment_ids": seg_ids}

        # 2) 分群引擎把授权成员和归属关联起来，提供实际纳入属性计算的对比数据
        data = engine.comparison_data(recipient, seg_ids, attr_key)

        # 3) 最少三名授权成员；越权成员已在引擎侧排除，无法偷偷补足样本
        if len(data) < MIN_AUTHORIZED:
            return {"ok": False, "delivered": False,
                    "reason": f"授权覆盖不足：至少需要 {MIN_AUTHORIZED} 名授权成员",
                    "authorized_count": len(data),  # 说明缺少覆盖（仅数量，不含属性）
                    "segment_ids": seg_ids}

        # 4) 反推防护：同一发布期不能再领可与已交付报告拼回小样本细组的另一份
        new_keys = {f"{d['segment_id']}:{d['member_key']}" for d in data}
        if self._conflicts_with_released(recipient, period, attr_key, new_keys):
            return {"ok": False, "delivered": False,
                    "reason": "与本期已交付报告叠加后可反推小样本细组，本期不再交付",
                    "segment_ids": seg_ids}

        # 5) 发布：只交付聚合结果；纳入的授权样本留存供拥有方核查
        stat_value = compute_controlled_average([d["value"] for d in data])
        db.insert("controlled_stat_releases", {
            "period": period,
            "recipient": recipient,
            "attr_key": attr_key,
            "segment_ids": json.dumps(seg_ids, ensure_ascii=False),
            "authorized_count": len(data),
            "stat_value": stat_value,
            "included_members": json.dumps(sorted(new_keys), ensure_ascii=False),
        })
        return {
            "ok": True, "delivered": True, "snapshot": False,
            "recipient": recipient, "period": period,
            "segment_ids": seg_ids, "attr_key": attr_key,
            "authorized_count": len(data),
            "stat_value": stat_value,
        }

    def audit_release(
        self,
        requester: str,
        recipient: str,
        period: str,
        segment_ids: list[int],
        attr_key: str,
    ) -> dict[str, Any]:
        """拥有方核查：某次对比输出实际纳入了哪些授权样本。"""
        if requester != OWNER:
            return {"ok": False, "message": "只有拥有方可以核查纳入样本"}
        seg_ids = sorted({int(s) for s in segment_ids})
        release = self._find_release(recipient, period, attr_key, seg_ids)
        if release is None:
            return {"ok": False, "message": "该发布期没有对应的已交付报告"}
        return {
            "ok": True,
            "recipient": recipient,
            "period": period,
            "segment_ids": seg_ids,
            "attr_key": attr_key,
            "authorized_count": release["authorized_count"],
            "stat_value": release["stat_value"],
            "included_members": json.loads(release["included_members"]),
        }

    def list_releases(self, recipient: str, period: str) -> list[dict[str, Any]]:
        """列出接收方在某发布期已领取的全部受控统计（聚合视图）。"""
        rows = db.fetchall(
            "SELECT * FROM controlled_stat_releases WHERE recipient=? AND period=? ORDER BY id",
            [recipient, period],
        )
        return [{
            "segment_ids": json.loads(r["segment_ids"]),
            "attr_key": r["attr_key"],
            "authorized_count": r["authorized_count"],
            "stat_value": r["stat_value"],
        } for r in rows]

    def _find_release(
        self,
        recipient: str,
        period: str,
        attr_key: str,
        seg_ids: list[int],
    ) -> dict[str, Any] | None:
        rows = db.fetchall(
            "SELECT * FROM controlled_stat_releases WHERE recipient=? AND period=? AND attr_key=?",
            [recipient, period, attr_key],
        )
        for row in rows:
            if json.loads(row["segment_ids"]) == seg_ids:
                return row
        return None

    def _conflicts_with_released(
        self,
        recipient: str,
        period: str,
        attr_key: str,
        new_keys: set[str],
    ) -> bool:
        """检查新报告与本期已交付报告叠加后是否会暴露小样本细组。

        均值是线性统计量：当两份报告的样本集合存在包含关系时，
        差集的均值可以被精确反推（差集和 = 大集合和 − 小集合和）。
        只要可反推出的差集不足 MIN_AUTHORIZED 名成员，就拒绝交付。
        """
        rows = db.fetchall(
            "SELECT included_members FROM controlled_stat_releases "
            "WHERE recipient=? AND period=? AND attr_key=?",
            [recipient, period, attr_key],
        )
        for row in rows:
            old_keys = set(json.loads(row["included_members"]))
            if old_keys == new_keys:
                continue  # 同一份报告的重开，不构成新披露
            if new_keys < old_keys:
                exposed = old_keys - new_keys
            elif old_keys < new_keys:
                exposed = new_keys - old_keys
            else:
                continue  # 无包含关系时，两份均值无法精确拼出差集
            if 0 < len(exposed) < MIN_AUTHORIZED:
                return True
        return False