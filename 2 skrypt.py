import pandas as pd

# === Funkcja czyszcząca nazwy kolumn (na małe litery i bez spacji) ===
def clean_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df

# === Funkcja zamieniająca 9. znak na "&" ===
def replace_9th_char(vin):
    if isinstance(vin, str) and len(vin) >= 9:
        return vin[:8] + "&" + vin[9:]
    return vin

# === 1. Wczytaj pliki ===
not_found = clean_columns(pd.read_csv("not_found_august.csv"))
wmi_results = clean_columns(pd.read_excel("wmi_results_202509081150.xlsx"))
years_1986 = clean_columns(pd.read_excel("vin_reference.xlsx", sheet_name="years_1986_2010"))
years_2011 = clean_columns(pd.read_excel("vin_reference.xlsx", sheet_name="years_2011_2026"))
exotics = clean_columns(pd.read_excel("vin_reference.xlsx", sheet_name="exotics"))

# UWAGA: dla arkusza 'incomplete' nie czyścimy nazw kolumn
incomplete = pd.read_excel("vin_reference.xlsx", sheet_name="incomplete")

# === 2. Usuń duplikaty VIN w not_found ===
not_found = not_found.drop_duplicates(subset=["vin"]).copy()

# === 3. Zamiana 9 znaku na "&" w not_found i exotics ===
not_found["vin_9replaced"] = not_found["vin"].apply(replace_9th_char)
exotics["vin_9replaced"] = exotics["vin"].apply(replace_9th_char)

# === 4. Usuń VIN-y z not_found, które występują w exotics ===
not_found = not_found[~not_found["vin_9replaced"].isin(exotics["vin_9replaced"])]

# === Wyciągamy WMI z not_found ===
not_found["wmi_code"] = not_found["vin"].str[:3]

# === Lista niedozwolonych WMI z incomplete ===
bad_wmi_list = incomplete["World Manufacturer Identifier (WMI)"].dropna().astype(str).str.strip()

# === Usuwamy rekordy z niedozwolonymi WMI ===
not_found = not_found[~not_found["wmi_code"].isin(bad_wmi_list)]

# === 5. Połącz zakresy lat ===
year_map = pd.concat([years_1986, years_2011], ignore_index=True)
year_map.columns = ["code", "year"]

# === 6. Filtruj wmi_results ===
wmi_filtered = wmi_results[
    wmi_results["vehicletype"].isin([
        "Truck",
        "Multipurpose Passenger Vehicle (MPV)",
        "Passenger Car"
    ])
].copy()

# === 7. Dodaj wmi_code ===
not_found["wmi_code"] = not_found["vin"].str[:3]
wmi_filtered["wmi_code"] = wmi_filtered["wmi"].str[:3]

# === 8. Merge ===
merged_df = not_found.merge(wmi_filtered, on="wmi_code", how="inner")

# === 9. VIN długości 10 ===
merged_df = merged_df[merged_df["vin"].str.len() == 10].copy()

# === 10. Filtr znaków VIN ===
invalid_10th = {"I", "O", "U", "Q", "Z", "0"}
merged_df["code_10"] = merged_df["vin"].str[9]
merged_df = merged_df[~merged_df["code_10"].isin(invalid_10th)]

merged_df["first_9"] = merged_df["vin"].str[:9]
merged_df = merged_df[~merged_df["first_9"].str.contains(r"[IOQ]", na=False)]

# === 11. Dopasuj możliwe lata ===
vin_with_years = merged_df.merge(year_map, left_on="code_10", right_on="code", how="inner")

# === 12. Zostaw tylko kolumny VIN i MODEL_YEAR (zmień nazwy kolumn) ===
final_output = vin_with_years[["vin", "year"]].copy()
final_output.columns = ["Vin", "Model Year"]  # ważne: duża litera V, spacja w "Model Year"

# === 13. Zapisz wynik do pliku CSV ===
final_output.to_csv("vin_with_years_output.csv", index=False, encoding="utf-8")  # bez BOM

print("Gotowe! Wynik zapisany do 'vin_with_years_output.csv'")

