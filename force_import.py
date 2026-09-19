import os,sys,uuid,httpx
from bs4 import BeautifulSoup
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DATABASE_URL","sqlite:///./aprolabs.db")
from app.database import SessionLocal,init_db
from app.models.passage import PipelineJob
from sqlalchemy import func
from app.services.split_combined_pdf import is_combined_exam,split_combined_exam
init_db()
db=SessionLocal()
UD="uploads/suneung"
H={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
T=[
(1453,"g3",2020,"수능"),(1479,"g3",2021,"3월 학력평가"),(1483,"g3",2021,"6월 모의평가"),
(1485,"g3",2021,"9월 모의평가"),(1495,"g3",2022,"3월 학력평가"),(1501,"g3",2022,"6월 모의평가"),
(1507,"g3",2022,"9월 모의평가"),(1509,"g3",2022,"수능"),(1510,"g3",2023,"3월 학력평가"),
(1516,"g3",2023,"6월 모의평가"),(1525,"g3",2023,"9월 모의평가"),(1617,"g3",2024,"4월 학력평가"),
(1618,"g3",2024,"6월 모의평가"),(1634,"g3",2024,"9월 모의평가"),(1646,"g3",2024,"10월 학력평가"),
(1663,"g3",2025,"3월 학력평가"),(1664,"g3",2025,"4월 학력평가"),(1665,"g3",2025,"6월 모의평가"),
(1480,"g2",2021,"3월 학력평가"),(1488,"g2",2021,"6월 모의평가"),(1489,"g2",2021,"9월 모의평가"),
(1615,"g2",2024,"3월 학력평가"),(1662,"g2",2025,"3월 학력평가"),
(1514,"g1",2023,"6월 모의평가"),(1523,"g1",2023,"9월 모의평가"),(1616,"g1",2024,"3월 학력평가"),
(1620,"g1",2024,"6월 모의평가"),(1609,"g1",2024,"11월 학력평가"),(1704,"g1",2026,"3월 학력평가"),
]
GM={"g3":"고3","g2":"고2","g1":"고1"}
ok=0
fail=0
for uid,g,yr,et in T:
    gr=GM[g]
    url="https://legendstudy.com/"+str(uid)
    print("")
    print("["+gr+" "+str(yr)+" "+et+"] "+url)
    try:
        r=httpx.get(url,headers=H,follow_redirects=True,timeout=30)
        soup=BeautifulSoup(r.text,"html.parser")
    except Exception as e:
        print("  X page error: "+str(e))
        fail+=1
        continue
    pdfs=[]
    for a in soup.select("a[href]"):
        h=a.get("href","")
        t=a.get_text(strip=True)
        if ".pdf" in h.lower() and t:
            has_korean = "국어" in t
            has_munje = "문제" in t
            is_answer = "해설" in t or "정답" in t or "등급" in t
            if has_korean and has_munje and not is_answer:
                pdfs.append({"url":h,"title":t})
    if not pdfs:
        print("  ! no korean pdf found")
        fail+=1
        continue
    for p in pdfs:
        title = p["title"][:60]
        print("  -> "+title)
        try:
            r2=httpx.get(p["url"],headers=H,follow_redirects=True,timeout=60)
            if r2.status_code!=200:
                print("  X HTTP "+str(r2.status_code))
                fail+=1
                continue
        except Exception as e:
            print("  X "+str(e))
            fail+=1
            continue
        jid=str(uuid.uuid4())
        fn=p["title"] if p["title"].endswith(".pdf") else p["title"]+".pdf"
        pp=os.path.join(UD,jid+".pdf")
        with open(pp,"wb") as f:
            f.write(r2.content)
        st="통합"
        if "언매" in fn:
            st="언매"
        elif "화작" in fn:
            st="화작"
        if st=="통합" and is_combined_exam(pp):
            print("  -> combined, splitting...")
            sps=split_combined_exam(pp,UD)
            if sps:
                for sp in sps:
                    sid=str(uuid.uuid4())
                    mn=db.query(func.max(PipelineJob.job_number)).scalar() or 0
                    db.add(PipelineJob(id=sid,job_number=mn+1,filename=sp["filename"],file_path=sp["path"],
                        source=url,source_year=yr,exam_type=et,subject="국어",
                        sub_type=sp["sub_type"],grade=gr,status="ready"))
                    db.commit()
                    print("    + "+sp["sub_type"])
                os.remove(pp)
                ok+=1
                continue
        mn=db.query(func.max(PipelineJob.job_number)).scalar() or 0
        db.add(PipelineJob(id=jid,job_number=mn+1,filename=fn,file_path=pp,
            source=url,source_year=yr,exam_type=et,subject="국어",
            sub_type=st,grade=gr,status="ready"))
        db.commit()
        print("  + registered")
        ok+=1
db.close()
print("")
print("Done: ok="+str(ok)+" fail="+str(fail))
