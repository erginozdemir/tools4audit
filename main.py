from fastapi import FastAPI, File, UploadFile, Request
import pandas as pd
from fastapi.responses import HTMLResponse, StreamingResponse
from io import BytesIO
from fastapi.templating import Jinja2Templates

app = FastAPI()

# HTML şablonları için Jinja2 kullanımı
templates = Jinja2Templates(directory="templates")

pivot_table_global = None  # Pivot tabloyu geçici olarak saklamak için

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """ Kullanıcıya Tools4Audit giriş sayfasını gösterir. """
    return templates.TemplateResponse("home.html", {"request": request})

@app.get("/aging_sample_download/")
async def download_sample():
    """ Örnek Excel dosyasını kullanıcıya sunar. """
    
    # Örnek veri oluştur
    sample_data = {
        "Hesap Kodu": ["120.01", "120.02", "320.01", "320.03"],
        "Hesap Adı": ["A Müşterisi", "B Müşterisi", "C Satıcısı", "D Satıcısı"],
        "Fiş Tarihi": ["2024-01-05", "2024-01-10", "2024-02-15", "2024-02-20"],
        "Fiş No": [123, 124, 221, 222],
        "Fiş Türü": ["Normal", "Açılış", "Normal", "Normal"],
        "Borç": [5000, 2000, 0, 3000],
        "Alacak": [0, 0, 4000, 0]
    }

    df = pd.DataFrame(sample_data)

    # Tarih formatını düzelt
    df["Fiş Tarihi"] = pd.to_datetime(df["Fiş Tarihi"])

    # Excel dosyasını oluştur
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="OrnekData", index=False)

    output.seek(0)

    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=ornek_yaslandirma.xlsx"})


@app.post("/aging/")
async def upload_file(request: Request, file: UploadFile = File(...)):
    global pivot_table_global

    contents = await file.read()
    excel_data = BytesIO(contents)

    df = pd.read_excel(excel_data)

    required_columns = ["Hesap Kodu", "Hesap Adı", "Fiş Tarihi", "Fiş No", "Fiş Türü", "Borç", "Alacak"]
    for col in required_columns:
        if col not in df.columns:
            return HTMLResponse(content=f"<h3>Hata: '{col}' sütunu eksik!</h3>", status_code=400)

    df["Fiş Tarihi"] = pd.to_datetime(df["Fiş Tarihi"], errors='coerce')

    # Sıralama: Hesap Kodu artan, Fiş Tarihi azalan
    df_sorted = df.sort_values(by=["Hesap Kodu", "Fiş Tarihi"], ascending=[True, False])

    # Bakiye sütunu hesaplama (Hesap Kodu'na göre Borç - Alacak toplamı)
    df_sorted["Bakiye"] = df_sorted.groupby("Hesap Kodu")["Borç"].transform("sum") - df_sorted.groupby("Hesap Kodu")["Alacak"].transform("sum")

    # Kalan sütunu hesaplama
    df_sorted["Kalan"] = 0  # Varsayılan olarak sıfırla
    hesap_kodu_onceki = None
    kalan_toplam = 0

    for i, row in df_sorted.iterrows():
        hesap_kodu = row["Hesap Kodu"]
        bakiye = row["Bakiye"]
        borc = row["Borç"]
        alacak = row["Alacak"] * -1  # Alacakları negatif olarak al

        if bakiye == 0:
            df_sorted.at[i, "Kalan"] = 0
        elif bakiye > 0:
            if hesap_kodu != hesap_kodu_onceki:  # Yeni hesap kodu başladı
                kalan_toplam = 0
                df_sorted.at[i, "Kalan"] = min(borc, bakiye)
            else:  # Aynı hesap kodu devam ediyor
                df_sorted.at[i, "Kalan"] = min(borc, bakiye - kalan_toplam)
        else:  # Bakiye < 0
            if hesap_kodu != hesap_kodu_onceki:  # Yeni hesap kodu başladı
                kalan_toplam = 0
                df_sorted.at[i, "Kalan"] = max(alacak, bakiye)
            else:  # Aynı hesap kodu devam ediyor
                df_sorted.at[i, "Kalan"] = max(alacak, bakiye - kalan_toplam)

        kalan_toplam += df_sorted.at[i, "Kalan"]
        hesap_kodu_onceki = hesap_kodu

    # Dönem sütunu hesaplama (Fiş Türü "Açılış" ise 0, değilse ay bilgisi)
    df_sorted["Dönem"] = df_sorted.apply(lambda row: 0 if row["Fiş Türü"] == "Açılış" else row["Fiş Tarihi"].month, axis=1)

    # Pivot tabloyu "Kalan" sütunu üzerinden oluştur
    pivot_table = pd.pivot_table(df_sorted, 
                                 values="Kalan",
                                 index=["Hesap Kodu", "Hesap Adı"],
                                 columns="Dönem",
                                 aggfunc="sum",
                                 fill_value=0,
                                 margins=True,  # Satır ve sütun toplamları için
                                 margins_name="Toplam")  # "All" yerine "Toplam" olarak adlandır

    # Sayı formatını uygula (nokta ile ayırma)
    pivot_table = pivot_table.applymap(lambda x: "{:,.0f}".format(x).replace(",", "."))

    pivot_columns = ["Hesap Kodu", "Hesap Adı"] + [str(col) for col in pivot_table.columns.tolist()]
    pivot_rows = pivot_table.reset_index().values.tolist()

    pivot_table_global = pivot_table  # Pivot tabloyu sakla

    return templates.TemplateResponse("aging.html", {
        "request": request, 
        "pivot_columns": pivot_columns, 
        "pivot_rows": pivot_rows
    })

@app.get("/aging_excel_download/")
async def download_excel():
    global pivot_table_global
    if pivot_table_global is None:
        return HTMLResponse(content="Henüz pivot tablo oluşturulmadı!", status_code=400)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        pivot_table_global.to_excel(writer, sheet_name="PivotTable")
    
    output.seek(0)

    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": "attachment; filename=pivot_table.xlsx"})
