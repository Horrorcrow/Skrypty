import pandas as pd

# Ścieżki do plików
final_list_path = r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\main-env\final list.csv"
be_eliminated_path = r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\main-env\be eliminated.xlsx"
output_path = r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\main-env\filtered_output.xlsx"

# Lista marek do usunięcia
makes_to_exclude = [
    "ACURA", "ASTON MARTIN", "BENTLEY", "FERRARI", "LAMBORGHINI",
    "MASERATI", "MCLAREN", "MERCEDES-BENZ", "POLESTAR", "ROLLS-ROYCE"
]

# Funkcja zamiany 9 znaku na &
def replace_9th_with_amp(vin):
    vin = str(vin)
    if len(vin) == 10:
        return vin[:8] + "&" + vin[9:]
    return vin  # jeśli coś jest nie tak z długością

# Wczytanie final list z BOM i czyszczeniem kolumn
df = pd.read_csv(final_list_path, encoding="utf-8-sig")
df.columns = df.columns.str.strip()

# Upewnienie się, że VIN jest stringiem
if "Vin" in df.columns:
    df["Vin"] = df["Vin"].astype(str)
elif "VIN" in df.columns:
    df["VIN"] = df["VIN"].astype(str)
    df.rename(columns={"VIN": "Vin"}, inplace=True)
else:
    raise KeyError("Kolumna VIN nie została znaleziona w pliku final list.")

# Tworzenie kolumn pomocniczych
df["VIN_ampersand"] = df["Vin"].apply(replace_9th_with_amp)
df["concat"] = df["Model Year"].astype(str) + df["VIN_ampersand"]

print(f"Final list wczytano: {len(df)} wierszy")

# 1) Usunięcie po Make
removed_make = df[df["Make"].str.upper().isin(makes_to_exclude)]
df = df[~df["Make"].str.upper().isin(makes_to_exclude)]
print(f"Usunięto po Make: {len(removed_make)} wierszy")

# Wczytanie pliku be eliminated
do_not_add = pd.read_excel(be_eliminated_path, sheet_name="DO NOT ADD")
models_from_two_sheets = pd.read_excel(be_eliminated_path, sheet_name="models from two sheets")
not_symbolled = pd.read_excel(be_eliminated_path, sheet_name="Not symbolled")

# VIN-y w DO NOT ADD — konwersja na string i ampersand
if "VIN" in do_not_add.columns:
    do_not_add["VIN"] = do_not_add["VIN"].astype(str)
elif "Vin" in do_not_add.columns:
    do_not_add.rename(columns={"Vin": "VIN"}, inplace=True)
    do_not_add["VIN"] = do_not_add["VIN"].astype(str)
else:
    raise KeyError("Brak kolumny VIN w DO NOT ADD")

do_not_add["VIN_ampersand"] = do_not_add["VIN"].apply(replace_9th_with_amp)

# 2) Usunięcie po VIN_ampersand z DO NOT ADD
removed_do_not_add = df[df["VIN_ampersand"].isin(do_not_add["VIN_ampersand"])]
df = df[~df["VIN_ampersand"].isin(do_not_add["VIN_ampersand"])]
print(f"Usunięto po DO NOT ADD (VIN_ampersand): {len(removed_do_not_add)} wierszy")

# 3) Usunięcie po Model z models from two sheets
df["Model_clean"] = df["Model"].astype(str).str.strip().str.upper()
models_to_exclude = models_from_two_sheets["Model"].astype(str).str.strip().str.upper()
removed_model = df[df["Model_clean"].isin(models_to_exclude)]
df = df[~df["Model_clean"].isin(models_to_exclude)]
print(f"Usunięto po Model: {len(removed_model)} wierszy")

# 4) Usunięcie po concat z Not symbolled
if "concat" not in not_symbolled.columns:
    not_symbolled["VIN"] = not_symbolled["VIN"].astype(str)
    not_symbolled["VIN_ampersand"] = not_symbolled["VIN"].apply(replace_9th_with_amp)
    not_symbolled["concat"] = not_symbolled["Model Year"].astype(str) + not_symbolled["VIN_ampersand"]

removed_not_symbolled = df[df["concat"].isin(not_symbolled["concat"])]
df = df[~df["concat"].isin(not_symbolled["concat"])]
print(f"Usunięto po Not symbolled (concat): {len(removed_not_symbolled)} wierszy")

# Zapis do Excela z wieloma arkuszami
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df.drop(columns=["Model_clean"], inplace=True)
    df.to_excel(writer, sheet_name="final_filtered", index=False)
    removed_make.to_excel(writer, sheet_name="removed_make", index=False)
    removed_do_not_add.to_excel(writer, sheet_name="removed_do_not_add", index=False)
    removed_model.to_excel(writer, sheet_name="removed_model", index=False)
    removed_not_symbolled.to_excel(writer, sheet_name="removed_not_symbolled", index=False)

print("Zapisano wynik do:", output_path)
