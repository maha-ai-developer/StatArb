# core/universe.py

import pandas as pd
import json
import os
from core.instrument_cache import load_instruments

UNIVERSE_FILE = "symbols.txt"
INDEX_JSON = "NSE_indices/nse_indices.json"

class UniverseBuilder:
    def __init__(self):
        self.instruments = load_instruments() or []
        self.df = pd.DataFrame(self.instruments)

        # normalize
        if "last_price" not in self.df:
            self.df["last_price"] = 0
        if "volume" not in self.df:
            self.df["volume"] = 0
        if "tradingsymbol" not in self.df:
            self.df["tradingsymbol"] = ""

    def top_by_price(self, n=50):
        df = self.df.sort_values("last_price", ascending=False)
        return df.head(n)["tradingsymbol"].tolist()

    def top_by_volume(self, n=50):
        df = self.df.sort_values("volume", ascending=False)
        return df.head(n)["tradingsymbol"].tolist()

    def load_csv_universe(self, csv_path):
        df = pd.read_csv(csv_path)
        
        # 1. Clean Headers: Strip whitespace/newlines
        possible_cols = [c for c in df.columns if c.strip().upper() in ["SYMBOL", "TICKER"]]
        
        if not possible_cols:
            raise Exception(f"CSV missing SYMBOL column. Found columns: {df.columns.tolist()}")
            
        # 2. Extract raw symbols
        raw_symbols = df[possible_cols[0]].dropna().astype(str).tolist()
        
        # 3. Clean Values & Filter:
        symbols = []
        for s in raw_symbols:
            clean_s = s.strip().upper()
            
            # --- IMPROVED FILTER LOGIC ---
            # 1. Skip Header-like values if they got in
            if clean_s in ["SYMBOL", "TICKER"]:
                continue
            
            # 2. Skip Indices (NIFTY 50, NIFTY 500, etc.)
            if "NIFTY" in clean_s:
                continue
                
            # 3. Skip empty strings
            if not clean_s:
                continue

            symbols.append(clean_s)
        
        # 4. Remove Duplicates
        symbols = sorted(list(set(symbols)))
        
        return symbols

    def load_index_universe(self, index_name):
        if not os.path.exists(INDEX_JSON):
            raise Exception(f"{INDEX_JSON} not found.")

        with open(INDEX_JSON, "r") as f:
            data = json.load(f)

        for cat in data.values():
            if isinstance(cat, list):
                for name in cat:
                    if name.lower() == index_name.lower():
                        return []

        raise Exception(f"Index '{index_name}' not found in NSE JSON")

    @staticmethod
    def save_symbols(symbols, file_path=UNIVERSE_FILE):
        with open(file_path, "w") as f:
            for s in symbols:
                f.write(s.strip() + "\n")
        print(f"[Universe] Saved {len(symbols)} symbols to {file_path}")

# --- CLI SUPPORT ---
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
        print(f"--- Processing {csv_file} ---")
        
        try:
            ub = UniverseBuilder()
            symbols = ub.load_csv_universe(csv_file)
            ub.save_symbols(symbols, "symbols.txt")
            print(f"✅ Done! Extracted {len(symbols)} valid stocks.")
        except Exception as e:
            print(f"❌ Error: {e}")
    else:
        print("Usage: python core/universe.py <csv_file>")
