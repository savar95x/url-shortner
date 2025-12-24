import string
import os
from typing import Optional
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import redis

# --- CONFIGURATION ---
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./shortener.db")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# --- DATABASE SETUP ---
connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class URLItem(Base):
    __tablename__ = "urls"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    original_url = Column(String, index=True)
    short_code = Column(String, unique=True, index=True)
    clicks = Column(Integer, default=0)

class AnalyticsItem(Base):
    __tablename__ = "analytics"
    id = Column(Integer, primary_key=True, index=True)
    short_code = Column(String, index=True)
    country = Column(String)

Base.metadata.create_all(bind=engine)

# --- REDIS SETUP ---
r_cache = redis.from_url(REDIS_URL, decode_responses=True)

# --- FASTAPI APP ---
app = FastAPI(title="The Scaler")

origins = [
        "http://localhost:5173",
        "https://url-shortner-backend-rose.vercel.app/",
        "https://apps.savar.is-a.dev",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- UTILITIES ---
BASE62 = string.digits + string.ascii_letters 

def base62_encode(num: int) -> str:
    if num == 0: return BASE62[0]
    arr = []
    base = len(BASE62)
    while num:
        num, rem = divmod(num, base)
        arr.append(BASE62[rem])
    arr.reverse()
    return ''.join(arr)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- ASYNC ANALYTICS ---
def record_analytics(short_code: str, country: str, db: Session):
    print(f"[Analytics] Link {short_code} clicked from {country}")
    try:
        # 1. Increment Counter
        url_item = db.query(URLItem).filter(URLItem.short_code == short_code).first()
        if url_item:
            url_item.clicks += 1
        
        # 2. Log Country
        log_entry = AnalyticsItem(short_code=short_code, country=country)
        db.add(log_entry)
        db.commit()
    except Exception as e:
        print(f"Analytics Error: {e}")

# --- ENDPOINTS ---
@app.get("/")
def health_check():
    return {"status": "online", "message": "The Scaler Backend is running!"}

class URLCreate(BaseModel):
    url: str

@app.get("/api/urls")
def get_all_urls(db: Session = Depends(get_db)):
    return db.query(URLItem).order_by(URLItem.id.desc()).limit(50).all()

@app.get("/api/analytics")
def get_analytics(db: Session = Depends(get_db)):
    results = db.query(AnalyticsItem.country, func.count(AnalyticsItem.country))\
        .group_by(AnalyticsItem.country).all()
    return [{"name": r[0], "clicks": r[1]} for r in results]

@app.get("/health")
async def health_check():
    return {"status": "active"}

@app.post("/shorten")
def shorten_url(item: URLCreate, db: Session = Depends(get_db)):
    # 0. CHECK IF EXISTS (Prevent Duplicates)
    existing_url = db.query(URLItem).filter(URLItem.original_url == item.url).first()
    
    if existing_url:
        # Return existing code, but mark 'existed' as True
        return {
            "short_code": existing_url.short_code, 
            "original": item.url, 
            "existed": True
        }

    # 1. Create DB Entry
    db_obj = URLItem(original_url=item.url)
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    
    # 2. Encode
    short_code = base62_encode(db_obj.id + 10000)
    
    # 3. Save Code
    db_obj.short_code = short_code
    db.commit()
    
    # 4. Cache
    try:
        r_cache.set(short_code, item.url, ex=3600)
    except Exception:
        pass 
    
    return {"short_code": short_code, "original": item.url, "existed": False}

@app.get("/{short_code}")
def redirect_to_url(
    short_code: str, 
    request: Request, 
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # 1. Redis Check
    target_url = None
    try:
        target_url = r_cache.get(short_code)
    except Exception:
        pass

    if not target_url:
        # 2. DB Check
        url_item = db.query(URLItem).filter(URLItem.short_code == short_code).first()
        if not url_item:
            raise HTTPException(status_code=404, detail="URL not found")
        target_url = url_item.original_url
        
        try:
            r_cache.set(short_code, target_url, ex=3600)
        except Exception:
            pass
    
    # 3. Analytics
    country = request.headers.get("CF-IPCountry", "Unknown") 
    background_tasks.add_task(record_analytics, short_code, country, db)
    
    return {"status": "302 Found", "location": target_url}
