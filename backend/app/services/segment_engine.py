"""分群引擎领域服务：按行为标签和属性条件自动生成分群结果。

真实数据流：定义分群条件 → 调用 pipeline.compute_segment 从 events 表匹配成员
→ 去重写入 segment_members 表 → 下游分群对比和漏斗分析消费。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import compute_segment
from app.services.base import BaseModuleService


class SegmentEngineService(BaseModuleService):
    MODULE = "segment_engine"
    ENTITY = "分群定义"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["分群名称", "分群条件", "所属人口特征", "预设大小", "最近更新时间"]
    REQUIRED_FIELDS = ["分群名称", "分群条件"]
    STATUS_ORDER = ["草稿", "计算中", "已生成", "已过期"]
    ACTION_RULES = {"启动计算": "已生成", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def run_computation(self, segment_id: int) -> dict[str, Any]:
        """执行分群计算。"""
        segment = db.fetchone("SELECT * FROM segments WHERE id=?", [segment_id])
        if segment is None:
            return {"ok": False, "message": f"分群 {segment_id} 不存在"}

        db.update("segments", {"status": "计算中"}, "id=?", [segment_id])
        count = compute_segment(dict(segment))
        return {
            "ok": True,
            "segment_name": segment["segment_name"],
            "members": count,
            "status": "已生成",
        }

    def list_members(self, segment_id: int, page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
        """列出分群成员。"""
        total = db.count("segment_members", "segment_id=?", [segment_id])
        rows = db.fetchall(
            "SELECT * FROM segment_members WHERE segment_id=? LIMIT ? OFFSET ?",
            [segment_id, size, (page - 1) * size],
        )
        return rows, total

    def member_overlap(self, segment_id_a: int, segment_id_b: int) -> dict[str, Any]:
        """计算两个分群的成员重叠度。"""
        members_a = {r["member_key"] for r in db.fetchall(
            "SELECT member_key FROM segment_members WHERE segment_id=?", [segment_id_a])}
        members_b = {r["member_key"] for r in db.fetchall(
            "SELECT member_key FROM segment_members WHERE segment_id=?", [segment_id_b])}

        intersection = members_a & members_b
        union = members_a | members_b
        jaccard = round(len(intersection) / max(len(union), 1), 4)
        overlap_a = round(len(intersection) / max(len(members_a), 1), 4)
        overlap_b = round(len(intersection) / max(len(members_b), 1), 4)

        return {
            "segment_a_size": len(members_a),
            "segment_b_size": len(members_b),
            "intersection": len(intersection),
            "jaccard_similarity": jaccard,
            "overlap_rate_a": overlap_a,
            "overlap_rate_b": overlap_b,
        }

    def attribute_summary(self, segment_id: int) -> dict[str, Any]:
        """汇总分群成员的属性分布。"""
        rows = db.fetchall(
            "SELECT member_attrs FROM segment_members WHERE segment_id=?", [segment_id])
        if not rows:
            return {"count": 0, "attributes": {}}

        from collections import Counter
        attr_counts: dict[str, Counter[str]] = {}
        for r in rows:
            try:
                attrs = json.loads(r["member_attrs"]) if isinstance(r["member_attrs"], str) else r.get("member_attrs", {})
                for k, v in attrs.items():
                    if k not in attr_counts:
                        attr_counts[k] = Counter()
                    attr_counts[k][str(v)] += 1
            except (json.JSONDecodeError, TypeError):
                pass

        return {
            "count": len(rows),
            "attributes": {
                k: dict(v.most_common(10)) for k, v in attr_counts.items()
            },
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_segments"] = db.count("segments")
        snapshot["total_members"] = db.count("segment_members")
        snapshot["generated_count"] = db.count("segments", "status='已生成'")
        return snapshot

    # ------------------------------------------------------------------
    # 受控统计：授权维度与业务范围
    # ------------------------------------------------------------------

    def grant_members(self, recipient: str, segment_id: int, member_keys: list[str]) -> dict[str, Any]:
        """拥有方把指定成员授权给接收方（授权维度）。

        只允许授权真实属于该分群的成员；重复授权自动去重。
        """
        granted = 0
        skipped: list[str] = []
        for key in member_keys:
            member = db.fetchone(
                "SELECT id FROM segment_members WHERE segment_id=? AND member_key=?",
                [segment_id, key],
            )
            if member is None:
                skipped.append(key)
                continue
            exists = db.fetchone(
                "SELECT id FROM segment_member_grants WHERE recipient=? AND segment_id=? AND member_key=?",
                [recipient, segment_id, key],
            )
            if exists:
                continue
            db.insert("segment_member_grants", {
                "recipient": recipient,
                "segment_id": segment_id,
                "member_key": key,
            })
            granted += 1
        return {
            "ok": True,
            "recipient": recipient,
            "segment_id": segment_id,
            "granted": granted,
            "skipped": skipped,
        }

    def set_recipient_scope(self, recipient: str, segment_ids: list[int]) -> dict[str, Any]:
        """设定接收方的业务范围：允许领取哪些归属分群的受控统计。"""
        db.delete("recipient_scopes", "recipient=?", [recipient])
        for sid in sorted({int(s) for s in segment_ids}):
            db.insert("recipient_scopes", {"recipient": recipient, "segment_id": sid})
        return {"ok": True, "recipient": recipient, "scope": sorted({int(s) for s in segment_ids})}

    def recipient_scope(self, recipient: str) -> set[int]:
        """接收方业务范围覆盖的归属分群集合。"""
        rows = db.fetchall(
            "SELECT segment_id FROM recipient_scopes WHERE recipient=?", [recipient])
        return {int(r["segment_id"]) for r in rows}

    def comparison_data(
        self,
        recipient: str,
        segment_ids: list[int],
        attr_key: str,
    ) -> list[dict[str, Any]]:
        """把授权成员和归属关联起来，向统计入口提供实际纳入属性计算的对比数据。

        只有同时满足两个维度的成员才会进入对比数据：
          1. 授权维度：成员已授权给该接收方（segment_member_grants）；
          2. 归属维度：成员属于本次请求的归属分群。
        未授权成员绝不出现，因此越权成员无法偷偷补足样本；
        返回的每一行就是统计入口实际纳入属性计算的样本。
        """
        seg_ids = sorted({int(s) for s in segment_ids})
        if not seg_ids:
            return []
        placeholders = ",".join("?" for _ in seg_ids)
        rows = db.fetchall(
            f"""
            SELECT g.segment_id AS segment_id, g.member_key AS member_key, m.member_attrs AS member_attrs
            FROM segment_member_grants g
            JOIN segment_members m
              ON m.segment_id = g.segment_id AND m.member_key = g.member_key
            WHERE g.recipient = ? AND g.segment_id IN ({placeholders})
            ORDER BY g.segment_id, g.member_key
            """,
            [recipient, *seg_ids],
        )
        data: list[dict[str, Any]] = []
        for row in rows:
            try:
                attrs = json.loads(row["member_attrs"]) if isinstance(row["member_attrs"], str) else (row["member_attrs"] or {})
            except (json.JSONDecodeError, TypeError):
                attrs = {}
            value = attrs.get(attr_key)
            if value is None:
                continue
            data.append({
                "segment_id": int(row["segment_id"]),
                "member_key": row["member_key"],
                "value": float(value),
            })
        return data

    def update_member_attr(self, segment_id: int, member_key: str, attr_key: str, value: float) -> dict[str, Any]:
        """更新成员属性值（拥有方侧操作，用于验证已发布快照不受后续变更影响）。"""
        row = db.fetchone(
            "SELECT member_attrs FROM segment_members WHERE segment_id=? AND member_key=?",
            [segment_id, member_key],
        )
        if row is None:
            return {"ok": False, "message": f"成员 {member_key} 不属于分群 {segment_id}"}
        try:
            attrs = json.loads(row["member_attrs"]) if isinstance(row["member_attrs"], str) else (row["member_attrs"] or {})
        except (json.JSONDecodeError, TypeError):
            attrs = {}
        attrs[attr_key] = value
        db.update(
            "segment_members",
            {"member_attrs": json.dumps(attrs, ensure_ascii=False)},
            "segment_id=? AND member_key=?",
            [segment_id, member_key],
        )
        return {"ok": True, "segment_id": segment_id, "member_key": member_key, attr_key: value}