"""Relational style presets, including an optional promoted Default theme."""
from sqlalchemy import Column, String, Integer, Float, ForeignKey, CheckConstraint, DateTime, func


def register(base):
    class AppearancePreset(base):
        __tablename__ = 'appearance_preset'
        id = Column(String(36), primary_key=True)
        name = Column(String(60), nullable=False)
        name_key = Column(String(60), unique=True, nullable=False)
        schema_version = Column(Integer, nullable=False, default=1)
        created_by = Column(String(36), ForeignKey('account.id', ondelete='SET NULL'))
        updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    class AppearanceSetting(base):
        __tablename__ = 'appearance_setting'
        id = Column(Integer, primary_key=True)
        revision = Column(Integer, nullable=False, default=0)
        active_preset_id = Column(String(36), ForeignKey('appearance_preset.id'))
        __table_args__ = (CheckConstraint('id = 1'),)

    class AppearanceOverride(base):
        __tablename__ = 'appearance_override'
        preset_id = Column(String(36), ForeignKey('appearance_preset.id', ondelete='CASCADE'), primary_key=True)
        scope = Column(String(48), primary_key=True)
        target = Column(String(64), primary_key=True)
        property = Column(String(32), primary_key=True)
        color_value = Column(String(9))
        number_value = Column(Float)
        text_value = Column(String(64))
        __table_args__ = (CheckConstraint('num_nonnulls(color_value, number_value, text_value) = 1'),)

    return AppearancePreset, AppearanceSetting, AppearanceOverride
