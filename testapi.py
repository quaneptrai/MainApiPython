import os
import uuid
import torch
import torch.nn.functional as F
import pyodbc
import json
import io
import re
import pdfplumber
import docx
import ollama  

from datetime import datetime
from typing import List, Optional,Literal
from groq import Groq
from fastapi import FastAPI, HTTPException, UploadFile, File, Form , Query
from pydantic import BaseModel
from openai import OpenAI
from pinecone import Pinecone
from FlagEmbedding import BGEM3FlagModel
from sentence_transformers import SentenceTransformer
 

app = FastAPI(title="Job & CV Smart Integrated API")

# ================= 1. CẤU HÌNH HỆ THỐNG =================
OPENAI_API_KEY = "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" 
PINECONE_API_KEY = "pcsk_3CPrZH_65UmbWfwGXiJYiFqzciZudoXx57Au2F1jNysFMvTAj5tZqbQriUtR7o8wRSGTda"
PINECONE_INDEX_NAME = "thuctap"

groq_client = Groq(api_key="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")

pc = Pinecone(api_key=PINECONE_API_KEY)
pc_index = pc.Index(PINECONE_INDEX_NAME)
ModelType = Literal["vntuan-long", "bge-m3", "jina", "openai"]
models_cache = {}

# --- KẾT NỐI SQL SERVER ---
def get_db_conn():
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=db47010.public.databaseasp.net;"
        "Database=db47010;"
        "UID=db47010;"
        "PWD=585810quan;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ================= 2. CORE LOGIC (LÀM SẠCH & EMBED) =================

def clean_text(text: str) -> str:
    """Làm sạch văn bản trước khi đưa vào model AI"""
    if not text: return ""
    text = text.lower()
    # Loại bỏ ký tự đặc biệt, giữ lại chữ Việt, số, dấu câu cơ bản
    text = re.sub(r'[^\w\s\.\,\-\/\:]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_embedding_1024(text: str, model_name: str):
    """Tạo vector 1024 chiều từ text đã làm sạch"""
    text = clean_text(text)
    
    if model_name == "bge-m3":
        if "bge-m3" not in models_cache:
            models_cache["bge-m3"] = BGEM3FlagModel('BAAI/bge-m3', use_fp16=True)
        return models_cache["bge-m3"].encode([text])['dense_vecs'][0].tolist()

    elif model_name == "jina": # Đảm bảo Jina cũng có trong logic xử lý
        if "jina" not in models_cache:
            models_cache["jina"] = SentenceTransformer('jinaai/jina-embeddings-v3', trust_remote_code=True)
        return models_cache["jina"].encode([text])[0].tolist()

    elif model_name == "openai":
        response = openai_client.embeddings.create(
            input=[text], model="text-embedding-3-small", dimensions=1024
        )
        return response.data[0].embedding

    elif model_name == "vntuan-long":
        if "vntuan-long" not in models_cache:
            models_cache["vntuan-long"] = SentenceTransformer('dangvantuan/vietnamese-embedding-long-context')
        emb_768 = models_cache["vntuan-long"].encode([text])
        emb_tensor = torch.tensor(emb_768)
        return F.pad(emb_tensor, (0, 1024 - 768), "constant", 0)[0].tolist()
    
    return get_embedding_1024(text, "vntuan-long")

# ================= 3. MODELS DỮ LIỆU (PYDANTIC) =================

class JobPosting(BaseModel):
    Title: str
    OriginalUrl: str
    Company: str = ""
    Locations: List[str] = []
    WorkTime: str = ""
    FullText: str = ""
    Salary: str = ""
    Deadline: str = ""
    Experience: str = ""
    Responsibilities: List[str] = []
    Requirements: List[str] = []
    Benefits: List[str] = []

# ================= 4. ENDPOINTS CHÍNH =================

@app.post("/upload-jobs-json", summary="Lưu embed trực tiếp từ JSON")
async def upload_jobs_json(
    file: UploadFile = File(..., description="Chọn file .json đã crawl"),
    model: ModelType = Form("vntuan-long", description="Chọn model để tạo vector")
):
    # 1. Đọc file JSON
    try:
        file_bytes = await file.read()
        jobs_data = json.loads(file_bytes)
        if isinstance(jobs_data, dict): jobs_data = [jobs_data]
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Lỗi đọc JSON: {str(e)}")

    conn = get_db_conn()
    cursor = conn.cursor()
    count = 0

    try:
        for item in jobs_data:
            try:
                job = JobPosting(**item)
            except:
                continue 

            # 2. CHỈ EMBED FULLTEXT (Lấy đúng cái cần dùng để tìm kiếm)
            text_to_embed = job.FullText if job.FullText.strip() else job.Title
            vector = get_embedding_1024(text_to_embed, model)

            # 3. ĐẨY LÊN PINECONE (Lưu embed)
            pc_index.upsert(vectors=[{
                "id": job.OriginalUrl, 
                "values": vector,
                "metadata": {
                    "type": "job",
                    "title": job.Title,
                    "company": job.Company,
                    "text": clean_text(job.FullText)[:1000] # Lưu preview để hiển thị khi search
                }
            }])

            # 4. LƯU VÀO SQL (Làm kho chứa thô)
            # Dùng MERGE hoặc kiểm tra nhanh để tránh crash nếu lỡ tay chạy lại file cũ
            sql = """
                IF NOT EXISTS (SELECT 1 FROM JobPostings WHERE OriginalUrl = ?)
                BEGIN
                    INSERT INTO JobPostings (Title, Company, WorkTime, Salary, Experience, OriginalUrl, FullText, CreatedAt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
                END
            """
            cursor.execute(sql, (job.OriginalUrl, job.Title, job.Company, job.WorkTime, job.Salary, job.Experience, job.OriginalUrl, job.FullText))
            
            count += 1

        conn.commit()
        return {
            "status": "success", 
            "processed": count,
            "message": f"Đã tạo embed và lưu thành công {count} công việc lên Pinecone & SQL."
        }
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/upload-cv-smart")
async def upload_cv_smart(
    employee_id: int = Form(...), 
    file: UploadFile = File(...), 
    model: ModelType = Form("vntuan-long")
):
    content = ""
    file_extension = file.filename.split('.')[-1].lower()
    file_bytes = await file.read()

    # 1. TRÍCH XUẤT VĂN BẢN (Phải làm bước này đầu tiên để lấy dữ liệu cho Prompt)
    try:
        if file_extension == "pdf":
            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                content = " ".join([page.extract_text() or "" for page in pdf.pages])
        elif file_extension in ["docx", "doc"]:
            doc = docx.Document(io.BytesIO(file_bytes))
            content = " ".join([p.text for p in doc.paragraphs])
        else:
            raise HTTPException(status_code=400, detail="Chỉ hỗ trợ PDF hoặc DOCX")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Lỗi đọc file: {str(e)}")

    if not content.strip():
        raise HTTPException(status_code=400, detail="Không tìm thấy nội dung văn bản trong CV")

    # 2. KHAI BÁO PROMPT (Gán giá trị cho biến prompt trước khi dùng)
    prompt = f"""
    Bạn là hệ thống trích xuất CV chuyên nghiệp. Hãy phân tích nội dung sau và trả về DUY NHẤT 1 đối tượng JSON.
    Nội dung CV: {content[:4000]}

    YÊU CẦU JSON:
    {{
      "is_student": true,
      "name": "Tên đầy đủ",
      "email": "Email liên hệ",
      "note": "Tóm tắt mục tiêu nghề nghiệp",
      "education": [
        {{ 
          "school": "Tên trường", 
          "field_of_study": "Ngành học", 
          "start_year": 2022, 
          "gpa": 3.1,
          "degree": "Cử nhân"
        }}
      ],
      "skills": ["Skill 1", "Skill 2"],
      "experiences": [
        {{ "company": "Tên cty", "position": "Vị trí", "duration": "Thời gian" }}
      ],
      "search_vector_content": "Viết 1 đoạn tóm tắt 100 chữ về kỹ năng và kinh nghiệm để tìm việc"
    }}
    """

    # 3. GỌI GROQ XỬ LÝ (Lúc này prompt đã có giá trị nên sẽ không lỗi nữa)
    try:
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {
                    "role": "system", 
                    "content": "Bạn là hệ thống trích xuất CV chuyên nghiệp. Trả về DUY NHẤT JSON."
                },
                {
                    "role": "user", 
                    "content": prompt 
                }
            ],
            model="llama-3.3-70b-versatile",
            response_format={"type": "json_object"}
        )
        
        # Parse kết quả JSON từ Groq
        structured_data = json.loads(chat_completion.choices[0].message.content)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi Groq: {str(e)}")

    # 4. TẠO VECTOR & LƯU PINECONE
    vector = get_embedding_1024(structured_data.get('search_vector_content', ''), model)
    
    pc_index.upsert(vectors=[{
        "id": f"acc_{employee_id}",
        "values": vector,
        "metadata": {
            "type": "cv",
            "is_student": structured_data.get('is_student', False),
            "name": structured_data.get('name', ''),
            "skills": ", ".join(structured_data.get('skills', []))
        }
    }])

    return {
        "status": "success",
        "employee_id": employee_id,
        "vector_id": f"acc_{employee_id}",
        "extracted_data": structured_data
    }
@app.post("/smart-match")
async def smart_match(text: str, target: str = "job", model: str = "vntuan-long"):
    """
    Tìm kiếm thông minh: Gửi text CV tìm Job hoặc ngược lại
    """
    cleaned_query = clean_text(text)
    vector = get_embedding_1024(cleaned_query, model)

    results = pc_index.query(
        vector=vector,
        top_k=5,
        include_metadata=True,
        filter={"type": {"$eq": target}}
    )

    matches = [{"score": round(res['score']*100, 2), "info": res['metadata']} for res in results['matches']]
    return {"results": matches}
@app.post("/jobposting")
def create_jobposting(jobs: List[JobPosting]):
    conn = None
    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        for job in jobs:
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
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
