import os
from pathlib import Path
import pandas as pd
import re
from datetime import datetime

# ----------------------------- #
# 1) Genel Ayarlar / Parametreler
# ----------------------------- #
KLASOR_YOLU   = Path(r"G:\Drive'ım\Statistic related\python course - BTK\OMÜ B puanı özel fark hesaplama aracı\Nucleus\Raw excels")
SKIPROWS      = 6

# Filtre parametreleri
B1_ALT_SINIR   = 50
B3_PUAN_EN_COK = 0  # puan_df içinde B3 > 0 olanları çıkarmak için eşik

HIZMET_DESENLERI = [
    "muayene",
    "konsültasyon",
    "sonda takılması",
    "santral ven kateterizasyonu, perkütan, periferik ven",
    "immünoterapi",
    "poliklinik özel mesai içi",
    "santral ven kateterizasyonu, juguler veya subklavyen ven",
    "nazogastrik",
    "otomatik/robotik infüzyon kemoterapisi",
    "kardiyopulmoner ressüsitasyon",
    "trakeal",
]

timestamp = datetime.now().strftime("%Y%m%d_%H%M")
OUTPUT_XLSX = KLASOR_YOLU / f"nucleus_rapor_{timestamp}.xlsx"

# ----------------------------- #
# 2) Yardımcı Fonksiyonlar
# ----------------------------- #
def list_xlsx(folder: Path) -> list[Path]:
    if not folder.exists():
        raise FileNotFoundError(f"Klasör bulunamadı: {folder}")
    files = sorted([p for p in folder.iterdir() if p.suffix.lower() == ".xlsx"])
    if not files:
        raise FileNotFoundError("Klasörde hiç .xlsx dosyası yok.")
    return files

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()
    return df

def ensure_score_columns(df: pd.DataFrame) -> pd.DataFrame:
    needed = [
        "hasta no", "hasta adı", "hizmet kodu", "hizmet adı", "adet",
        "b1 (puan)", "b2 (puan)", "b3 (puan)", "hizmet tarihi",
        "özel fark var", "özel fark tipi", "özel fark tutarı"
    ]
    for col in needed:
        if col not in df.columns:
            if col in {"b1 (puan)", "b2 (puan)", "b3 (puan)", "adet", "özel fark tutarı"}:
                df[col] = 0
            else:
                df[col] = pd.NA
    for col in ["b1 (puan)", "b2 (puan)", "b3 (puan)", "adet", "özel fark tutarı"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    if "hizmet tarihi" in df.columns:
        df["hizmet tarihi"] = pd.to_datetime(df["hizmet tarihi"], errors="coerce")
    return df[needed]

def read_and_clean_excel(file_path: Path, skiprows: int = 6) -> pd.DataFrame:
    df = pd.read_excel(file_path, skiprows=skiprows, engine="openpyxl")
    df = clean_columns(df)
    df = ensure_score_columns(df)
    return df

def compile_service_regex(patterns: list[str]) -> re.Pattern:
    escaped = [re.escape(p) for p in patterns]
    return re.compile("|".join(escaped), flags=re.IGNORECASE)

def humanize_int(x: int | float) -> str:
    try:
        return f"{x:,.0f}".replace(",", ".")
    except Exception:
        return str(x)

# ----------------------------- #
# 3) Dosyaları Listele ve Seçim Al
# ----------------------------- #
files = list_xlsx(KLASOR_YOLU)
print("Klasördeki dosyalar:")
for i, p in enumerate(files):
    print(f"{i}: {p.name}")

puan_index   = int(input("\nPuan dosyasının index numarasını girin: "))
fatura_index = int(input("Fatura dosyasının index numarasını girin: "))

puan_path   = files[puan_index]
fatura_path = files[fatura_index]

# ----------------------------- #
# 4) Veriyi Oku ve Düzelt
# ----------------------------- #
puan_df_raw   = read_and_clean_excel(puan_path, skiprows=SKIPROWS)
fatura_df_raw = read_and_clean_excel(fatura_path, skiprows=SKIPROWS)

for df in [puan_df_raw, fatura_df_raw]:
    mask_var = (
        (df["özel fark var"].astype(str).str.lower() == "var") &
        (df["b3 (puan)"] == 0)
    )
    b1_pos = df["b1 (puan)"] > 0
    b2_pos = df["b2 (puan)"] > 0
    df.loc[mask_var & b1_pos, "b3 (puan)"] = df.loc[mask_var & b1_pos, "b1 (puan)"]
    df.loc[mask_var & b1_pos, "b1 (puan)"] = 0
    df.loc[mask_var & ~b1_pos & b2_pos, "b3 (puan)"] = df.loc[mask_var & ~b1_pos & b2_pos, "b2 (puan)"]
    df.loc[mask_var & ~b1_pos & b2_pos, "b2 (puan)"] = 0

# ----------------------------- #
# 5) Filtreleme
# ----------------------------- #
counts = {}
counts["puan_baslangic"]   = len(puan_df_raw)
counts["fatura_baslangic"] = len(fatura_df_raw)

# Puan DF: B3>0 çıkar
puan_df = puan_df_raw[puan_df_raw["b3 (puan)"] <= B3_PUAN_EN_COK]
counts["puan_b3_cikarilan"] = counts["puan_baslangic"] - len(puan_df)

# Birleştir
birlesik_df = pd.concat([puan_df, fatura_df_raw], ignore_index=True, copy=False)
counts["birlesik_toplam"] = len(birlesik_df)

# Düşük puan kayıtlarını çıkar
mask_low_scores = (
    (birlesik_df["b1 (puan)"] < B1_ALT_SINIR) &
    (birlesik_df["b2 (puan)"] == 0) &
    (birlesik_df["b3 (puan)"] == 0)
)
counts["low_score_cikarilan"] = int(mask_low_scores.sum())
birlesik_df = birlesik_df[~mask_low_scores]

# Hizmet adı filtreleri
svc_re = compile_service_regex(HIZMET_DESENLERI)
mask_services = birlesik_df["hizmet adı"].fillna("").str.contains(svc_re)
counts["hizmet_adi_cikarilan"] = int(mask_services.sum())
birlesik_df = birlesik_df[~mask_services]

counts["nihai_satir"] = len(birlesik_df)

# ----------------------------- #
# 6) Özet Hesaplama
# ----------------------------- #
b1_toplam = float(birlesik_df["b1 (puan)"].sum())
b2_toplam = float(birlesik_df["b2 (puan)"].sum())
b3_toplam = float(birlesik_df["b3 (puan)"].sum())
ozel_denge = b1_toplam + b2_toplam - b3_toplam

# **YENİ:** Özel fark tutarı toplamı (sadece fatura dosyasından)
ozel_fark_toplami_fatura = float(fatura_df_raw["özel fark tutarı"].sum())

# Rapor DataFrame
rapor_df = pd.DataFrame([
    ("Puan DF başlangıç", counts["puan_baslangic"]),
    ("Fatura DF başlangıç", counts["fatura_baslangic"]),
    ("Birleştirilmiş (puan+b3≤0 + fatura)", counts["birlesik_toplam"]),
    ("B3>0 çıkarılan (puan DF)", counts["puan_b3_cikarilan"]),
    ("Düşük puanla çıkarılan", counts["low_score_cikarilan"]),
    ("Hizmet adı ile çıkarılan", counts["hizmet_adi_cikarilan"]),
    ("Nihai (filtre sonrası)", counts["nihai_satir"]),
    ("—", "—"),
    ("B1 Toplamı", b1_toplam),
    ("B2 Toplamı", b2_toplam),
    ("B3 Toplamı", b3_toplam),
    ("Özel Denge (B1 + B2 - B3)", ozel_denge),
    ("Özel Fark Tutarı (Fatura)", ozel_fark_toplami_fatura),
], columns=["Metrik", "Değer"])

# ----------------------------- #
# 7) Konsol Çıktısı
# ----------------------------- #
print("\n--- FİLTRE ÖNCESİ/SONRASI SAYIM ---")
for k, v in counts.items():
    print(f"{k}: {humanize_int(v)}")

print("\n--- HESAPLAMA SONUÇLARI ---")
print(f"B1 Toplamı: {b1_toplam:,.0f}")
print(f"B2 Toplamı: {b2_toplam:,.0f}")
print(f"B3 Toplamı: {b3_toplam:,.0f}")
print(f"Özel Denge (B1 + B2 - B3): {ozel_denge:,.0f}")
print(f"Özel Fark Tutarı (yalnız Fatura): {ozel_fark_toplami_fatura:,.0f}")

# ----------------------------- #
# 8) Rapor Oluşturma Onayı
# ----------------------------- #
cevap = input("\nExcel raporu oluşturulsun mu? (E/h): ").strip().lower()
if cevap in ("e", "evet", "y", "yes"):
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        # Ham veriler
        puan_df_raw.to_excel(writer, index=False, sheet_name="Ham_Puan")
        fatura_df_raw.to_excel(writer, index=False, sheet_name="Ham_Fatura")
        # Filtreli birleşik veri
        birlesik_df.to_excel(writer, index=False, sheet_name="Filtreli")
        # Özet
        rapor_df.to_excel(writer, index=False, sheet_name="Özet")

        workbook = writer.book
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            for column_cells in worksheet.columns:
                max_len = max((len(str(cell.value)) for cell in column_cells), default=0)
                col_letter = column_cells[0].column_letter
                worksheet.column_dimensions[col_letter].width = max_len + 2
    print(f"\nExcel raporu kaydedildi: {OUTPUT_XLSX}")
else:
    print("\nRapor oluşturulmadı (kullanıcı tercihi).")
