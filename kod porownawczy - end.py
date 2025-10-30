import pandas as pd


#Load Excel
file_path=r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\AUGUST.xlsx"
try:
    df = pd.read_excel(file_path, engine='openpyxl')
    print("Excel file loaded.")
except Exception as e:
    print(f"Error loading Excel: {e}")
    exit()

    #tymczasowa kolumna ktora laczy Vin i Model Year obie wartosci zamienione na stringi

main_column = df["Model Year"].astype(str) + "_" + df["VIN"].astype(str)
# Kolumna do wpisania miesiąca, początkowo pusta
df["Match"] = ""


pliki_miesiace = [r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\FEBRUARY.xlsx", 
                    r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\MARCH.xlsx", 
                    r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\APRIL.xlsx",
                    r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\MAY.xlsx",
                    r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\JUNE.xlsx",
                    r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\JULY.xlsx",
                    ]

for miesiac in pliki_miesiace:
    temp_df = pd.read_excel(miesiac, engine='openpyxl')
    temp_column = temp_df["Model Year"].astype(str) + "_" + temp_df["VIN"].astype(str)
    
    # Nazwa miesiąca z kolumny MONTH pliku porównawczego
    month_name = temp_df["MONTH"].iloc[0]

    # Sprawdzamy, które wartości z main_column występują w temp_column
    mask = main_column.isin(temp_column)
    
    # Wpisujemy nazwę miesiąca tylko tam, gdzie dopasowanie jest prawdziwe i kolumna jest pusta
    df.loc[mask & (df["Match"] == ""), "Match"] = month_name

output_path = r"C:\Users\I45560\OneDrive - Verisk Analytics\Desktop\outcomes from VINs\AUGUST_matched.xlsx"
df.to_excel(output_path, index=False)
print("Wynik zapisany do nowego pliku Excel.")
