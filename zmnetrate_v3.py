import os
import sys
import json
import pandas as pd
from typing import Optional, Set

ACTIVE_DIR = "ActiveRateSheets_Origin"

# Partial E.164 map (extend as needed)
E164_CODES = {
    "1": "USA/Canada",
    "1242": "Bahamas",
    "1246": "Barbados",
    "1264": "Anguilla",
    "1268": "Antigua and Barbuda",
    "1284": "British Virgin Islands",
    "1286": "Anguilla",
    "1345": "Cayman Islands",
    "1441": "Bermuda",
    "1473": "Grenada",
    "1649": "Turks and Caicos",
    "1664": "Montserrat",
    "1670": "Northern Mariana Islands",
    "1671": "Guam",
    "1684": "American Samoa",
    "1758": "Saint Lucia",
    "1767": "Dominica",
    "1784": "Saint Vincent and the Grenadines",
    "1787": "Puerto Rico",
    "1809": "Dominican Republic",
    "1868": "Trinidad and Tobago",
    "1869": "Sa Kitts and Nevis",
    "1876": "Jamaica",
    "20": "Egypt",
    "27": "South Africa",
    "30": "Greece",
    "31": "Netherlands",
    "32": "Belgium",
    "33": "France",
    "34": "Spain",
    "36": "Hungary",
    "39": "Italy",
    "40": "Romania",
    "41": "Switzerland",
    "43": "Austria",
    "44": "United Kingdom",
    "45": "Denmark",
    "46": "Sweden",
    "47": "Norway",
    "48": "Poland",
    "49": "Germany",
    "51": "Peru",
    "52": "Mexico",
    "53": "Cuba",
    "54": "Argentina",
    "55": "Brazil",
    "56": "Chile",
    "57": "Colombia",
    "58": "Venezuela",
    "60": "Malaysia",
    "61": "Australia",
    "62": "Indonesia",
    "63": "Philippines",
    "64": "New Zealand",
    "65": "Singapore",
    "66": "Thailand",
    "81": "Japan",
    "82": "South Korea",
    "84": "Vietnam",
    "86": "China",
    "90": "Turkey",
    "91": "India",
    "92": "Pakistan",
    "93": "Afghanistan",
    "94": "Sri Lanka",
    "95": "Myanmar",
    "98": "Iran",
    "211": "South Sudan",
    "212": "Morocco",
    "213": "Algeria",
    "216": "Tunisia",
    "218": "Libya",
    "220": "Gambia",
    "221": "Senegal",
    "222": "Mauritania",
    "223": "Mali",
    "224": "Guinea",
    "225": "Ivory Coast",
    "226": "Burkina Faso",
    "227": "Niger",
    "228": "Togo",
    "229": "Benin",
    "230": "Mauritius",
    "231": "Liberia",
    "232": "Sierra Leone",
    "233": "Ghana",
    "234": "Nigeria",
    "235": "Chad",
    "236": "Central African Republic",
    "237": "Cameroon",
    "238": "Cape Verde",
    "239": "São Tomé and Príncipe",
    "240": "Equatorial Guinea",
    "241": "Gabon",
    "242": "Congo (Republic)",
    "243": "Congo (DRC)",
    "244": "Angola",
    "248": "Seychelles",
    "249": "Sudan",
    "250": "Rwanda",
    "251": "Ethiopia",
    "252": "Somalia",
    "253": "Djibouti",
    "254": "Kenya",
    "255": "Tanzania",
    "256": "Uganda",
    "257": "Burundi",
    "258": "Mozambique",
    "260": "Zambia",
    "261": "Madagascar",
    "262": "Réunion",
    "263": "Zimbabwe",
    "264": "Namibia",
    "265": "Malawi",
    "266": "Lesotho",
    "267": "Botswana",
    "268": "Eswatini",
    "269": "Comoros",
    "290": "Saint Helena",
    "291": "Eritrea",
    "297": "Aruba",
    "298": "Faroe Islands",
    "299": "Greenland",
    "350": "Gibraltar",
    "351": "Portugal",
    "352": "Luxembourg",
    "353": "Ireland",
    "354": "Iceland",
    "355": "Albania",
    "356": "Malta",
    "357": "Cyprus",
    "358": "Finland",
    "359": "Bulgaria",
    "370": "Lithuania",
    "371": "Latvia",
    "372": "Estonia",
    "373": "Moldova",
    "374": "Armenia",
    "375": "Belarus",
    "376": "Andorra",
    "377": "Monaco",
    "378": "San Marino",
    "380": "Ukraine",
    "381": "Serbia",
    "382": "Montenegro",
    "385": "Croatia",
    "386": "Slovenia",
    "387": "Bosnia and Herzegovina",
    "389": "North Macedonia",
    "420": "Czech Republic",
    "421": "Slovakia",
    "423": "Liechtenstein"
    # (extend further if needed)
}


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    colmap = {"origincode": "OriginCode", "dialcode": "DialCode", "rate": "Rate"}
    df = df.rename(columns={c: colmap[c] for c in df.columns if c in colmap})
    required = {"OriginCode", "DialCode", "Rate"}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns {required}, found {list(df.columns)}")
    df["OriginCode"] = df["OriginCode"].astype(str).str.strip().str.upper()
    df["DialCode"] = df["DialCode"].astype(str).str.strip()
    df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce")
    df = df.dropna(subset=["Rate"])
    return df[["OriginCode", "DialCode", "Rate"]]

def _longest_prefix_in_set(number_str: str, candidates: Set[str]) -> Optional[str]:
    s = str(number_str)
    for length in range(len(s), 0, -1):
        p = s[:length]
        if p in candidates:
            return p
    return None

def _e164_country_code(number: str) -> Optional[str]:
    s = str(number)
    for length in (4, 3, 2, 1):  # check longer NANP prefixes first
        p = s[:length]
        if p in E164_CODES:
            return p
    return None

def _pick_best_for_vendor(df: pd.DataFrame, calling_number: str, called_number: str, vendor_name: str, dbg):
    origins = set(df["OriginCode"].astype(str).unique())
    origin_candidates = sorted(
        [o for o in origins if o != "ALL" and str(calling_number).startswith(o)],
        key=lambda x: -len(x)
    )
    if "ALL" in origins:
        origin_candidates.append("ALL")

    dbg["origin_candidates"] = origin_candidates

    for oc in origin_candidates:
        sub = df[df["OriginCode"] == oc].copy()
        if sub.empty:
            dbg["attempts"].append({"origin": oc, "note": "no rows"})
            continue
        dial_set = set(sub["DialCode"].astype(str).unique())
        dial = _longest_prefix_in_set(str(called_number), dial_set)
        if not dial:
            dbg["attempts"].append({"origin": oc, "note": "no dial match"})
            continue
        best_row = sub[sub["DialCode"] == dial].sort_values("Rate", ascending=True).iloc[0]
        dbg["selected"] = {"origin": oc, "dial": dial, "rate": float(best_row["Rate"])}
        return {
            "vendor": vendor_name,
            "origin": oc,
            "prefix": dial,
            "rate": float(best_row["Rate"])
        }

    dbg["selected"] = None
    return None

def find_best_vendors(calling_number: str, called_number: str, carrier: str):
    overall_debug = {"vendors": []}
    results = []

    if not os.path.exists(ACTIVE_DIR):
        return {"status": "error", "details": f"Folder {ACTIVE_DIR} not found"}

    # CallType detection (E.164)
    call_cc = _e164_country_code(calling_number)
    dest_cc = _e164_country_code(called_number)
    if call_cc and dest_cc and call_cc == dest_cc:
        calltype = "NATL"
        partition = f"{carrier}_NATL"
    else:
        calltype = "ILD"
        partition = "ZOOM_NATIVE"

    # Evaluate each vendor
    for fname in os.listdir(ACTIVE_DIR):
        if not fname.lower().endswith(".csv"):
            continue
        vendor = fname.split("_")[0].upper()
        fpath = os.path.join(ACTIVE_DIR, fname)

        vdbg = {"vendor": vendor, "attempts": []}
        try:
            raw = pd.read_csv(fpath, dtype=str).fillna("")
            df = _normalize_df(raw)
            pick = _pick_best_for_vendor(df, calling_number, called_number, vendor, vdbg)
            if pick:
                results.append(pick)
            else:
                vdbg.setdefault("note", "no match")
        except Exception as e:
            vdbg["error"] = str(e)

        overall_debug["vendors"].append(vdbg)

    # Sort vendors by rate
    results.sort(key=lambda x: x["rate"])
    formatted = []
    for i, r in enumerate(results, 1):
        formatted.append({
            f"vendor{i}": r["vendor"],
            "origin": r["origin"],
            "prefix": r["prefix"],
            "rate": r["rate"],
            "priority": i
        })

    # CallType Indicator logic
    if calltype == "ILD":
        if formatted:
            calltype_indicator = "ILD_V_LIST"
        else:
            calltype_indicator = "ILD_NO_V_LIST"
    else:
        calltype_indicator = "NATL"

    return {
        "calling_number": calling_number,
        "called_number": called_number,
        "status": "success",
        "CallType": calltype,
        "Partition": partition,
        "CallType_Indicator": calltype_indicator,
        "leastCostVendors": formatted,
        "debug": overall_debug
    }

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 zmnetrate_v3.py <calling_number> <called_number> <carrier>")
        sys.exit(1)

    calling_number, called_number, carrier = sys.argv[1:4]
    out = find_best_vendors(calling_number, called_number, carrier)
    print(json.dumps(out, indent=2))
