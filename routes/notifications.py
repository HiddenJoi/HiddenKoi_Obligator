"""
Notification endpoints.
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from auth import get_current_user
from notifications import (
    db_list_notifications,
    db_mark_notification_read,
    db_mark_all_read,
    db_delete_notification,
    db_get_notification_settings,
    db_update_notification_settings,
    generate_notifications,
)

router = APIRouter(prefix="/notifications", tags=["notifications"])


# ── Response models ────────────────────────────────────────────────────────────

class NotificationResponse(BaseModel):
    id: int
    user_id: int
    type: str
    title: str
    message: str
    is_read: bool
    created_at: str


class NotificationListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    unread_only: bool
    items: List[NotificationResponse]


class NotificationSettingsResponse(BaseModel):
    coupon_alerts: bool
    risk_alerts: bool
    target_alerts: bool
    target_threshold: float


class UpdateSettingsRequest(BaseModel):
    coupon_alerts: Optional[bool] = None
    risk_alerts: Optional[bool] = None
    target_alerts: Optional[bool] = None
    target_threshold: Optional[float] = None


class GenerateResponse(BaseModel):
    created: dict


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("", response_model=NotificationListResponse)
def list_notifications(
    user:        dict = Depends(get_current_user),
    limit:       int = Query(default=50, ge=1, le=100),
    offset:      int = Query(default=0, ge=0),
    unread_only: bool = Query(default=False),
):
    """GET /notifications — список уведомлений текущего пользователя."""
    rows = db_list_notifications(user["id"], limit=limit, offset=offset, unread_only=unread_only)
    return NotificationListResponse(
        total=len(rows),
        limit=limit,
        offset=offset,
        unread_only=unread_only,
        items=[
            NotificationResponse(
                id=r["id"],
                user_id=r["user_id"],
                type=r["type"],
                title=r["title"],
                message=r["message"],
                is_read=r["is_read"],
                created_at=r["created_at"].isoformat() if r["created_at"] else "",
            )
            for r in rows
        ],
    )


@router.post("/read/{notification_id}", response_model=NotificationResponse)
def mark_read(
    notification_id: int,
    user: dict = Depends(get_current_user),
):
    """POST /notifications/read/{id} — отметить уведомление как прочитанное."""
    result = db_mark_notification_read(user["id"], notification_id)
    if not result:
        raise HTTPException(status_code=404, detail="Notification not found")
    return NotificationResponse(
        id=result["id"],
        user_id=result["user_id"],
        type=result["type"],
        title=result["title"],
        message=result["message"],
        is_read=result["is_read"],
        created_at=result["created_at"].isoformat() if result["created_at"] else "",
    )


@router.post("/read-all")
def mark_all_read(user: dict = Depends(get_current_user)):
    """POST /notifications/read-all — отметить все уведомления как прочитанные."""
    count = db_mark_all_read(user["id"])
    return {"marked_read": count}


@router.delete("/{notification_id}")
def delete_notification(
    notification_id: int,
    user: dict = Depends(get_current_user),
):
    """DELETE /notifications/{id} — удалить уведомление."""
    deleted = db_delete_notification(user["id"], notification_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Notification not found")
    return {"deleted": True}


@router.get("/settings", response_model=NotificationSettingsResponse)
def get_settings(user: dict = Depends(get_current_user)):
    """GET /notifications/settings — получить настройки уведомлений."""
    settings = db_get_notification_settings(user["id"])
    return NotificationSettingsResponse(
        coupon_alerts=settings["coupon_alerts"],
        risk_alerts=settings["risk_alerts"],
        target_alerts=settings["target_alerts"],
        target_threshold=float(settings["target_threshold"]),
    )


@router.patch("/settings", response_model=NotificationSettingsResponse)
def update_settings(
    body: UpdateSettingsRequest,
    user: dict = Depends(get_current_user),
):
    """PATCH /notifications/settings — обновить настройки уведомлений."""
    updated = db_update_notification_settings(
        user_id=user["id"],
        coupon_alerts=body.coupon_alerts,
        risk_alerts=body.risk_alerts,
        target_alerts=body.target_alerts,
        target_threshold=body.target_threshold,
    )
    return NotificationSettingsResponse(
        coupon_alerts=updated["coupon_alerts"],
        risk_alerts=updated["risk_alerts"],
        target_alerts=updated["target_alerts"],
        target_threshold=float(updated["target_threshold"]),
    )


@router.post("/generate", response_model=GenerateResponse)
def run_generate(user: dict = Depends(get_current_user)):
    """POST /notifications/generate — запустить генерацию уведомлений для пользователя."""
    created = generate_notifications(user["id"])
    return GenerateResponse(created=created)
