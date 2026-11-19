"""Shared session factory; the runtime composition root validates before binding."""
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

AsyncDBSession = sessionmaker(expire_on_commit=False, class_=AsyncSession)
