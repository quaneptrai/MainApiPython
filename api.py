from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import pyodbc
import json
from typing import List
from datetime import datetime
app = FastAPI(title="JobPosting API")

# Pydantic model để validate JSON
class JobPosting(BaseModel):
    Title: str
    Company: str = ""
    Locations: List[str] = []         # list full address, kiểu JSON
    LocationTags: List[str] = []      # list city
    WorkTime: str
    Responsibilities: List[str] = []
    Requirements: List[str] = []
    Benefits: List[str] = []
    Tags: Dict[str, List[str]] = {}
    FullText: str = ""
    Salary: str = ""
    Deadline: str = ""               # format "dd/mm/yyyy"
    Experience: str = ""

# Kết nối SQL Server LocalDB
def get_conn():
    conn_str = (
        r"Driver={ODBC Driver 18 for SQL Server};"
        r"Server=(localdb)\MSSQLLocalDB;"
        r"Database=Tester;"
        r"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

# Route root để test server
@app.get("/")
def root():
    return {"message": "JobPosting API is running!"}

# API POST để lưu JSON vào DB
@app.post("/jobposting")
def create_jobposting(jobs: List[JobPosting]):
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()

        for job in jobs:
            # 1. Xử lý Deadline (Chuyển "dd/mm/yyyy" -> ISO format cho SQL)
            deadline_dt = None
            if job.Deadline:
                try:
                    deadline_dt = datetime.strptime(job.Deadline, "%d/%m/%Y")
                except ValueError:
                    deadline_dt = None

            # 2. Serialize các trường JSON string (khớp với nvarchar(max) trong C#)
            responsibilities_json = json.dumps(job.Responsibilities, ensure_ascii=False)
            requirements_json = json.dumps(job.Requirements, ensure_ascii=False)
            benefits_json = json.dumps(job.Benefits, ensure_ascii=False)
            location_tags_json = json.dumps(job.LocationTags, ensure_ascii=False)

            # 3. INSERT vào bảng JobPostings 
            # LƯU Ý: Không có cột 'Locations' ở đây
            cursor.execute("""
                INSERT INTO JobPostings 
                (Title, Company, Responsibilities, Requirements, Benefits, WorkTime, 
                 Salary, Experience, Deadline, LocationTags, FullText, CreatedAt, UpdatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE())
            """, 
            job.Title, job.Company, responsibilities_json, requirements_json, 
            benefits_json, job.WorkTime, job.Salary, job.Experience, 
            deadline_dt, location_tags_json, job.FullText)

            # 4. Lấy Id vừa sinh ra (Identity)
            job_id = cursor.execute("SELECT @@IDENTITY").fetchval()

            # 5. Insert vào bảng JobLocations (Quan hệ 1-N)
            for loc_str in job.Locations:
                city = ""
                address = loc_str
                
                if ":" in loc_str:
                    parts = loc_str.split(":", 1)
                    city = parts[0].strip("-").strip()
                    address = parts[1].strip()

                cursor.execute("""
                    INSERT INTO JobLocations (JobPostingId, City, Address)
                    VALUES (?, ?, ?)
                """, job_id, city, address)

        conn.commit()
        return {"status": "success", "message": f"Inserted {len(jobs)} jobs"}

    except Exception as e:
        if conn: conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn: conn.close()