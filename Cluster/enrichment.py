"""
Site enrichment pipeline.

Derives features from Address and Postcode columns already present in the
main energy CSV, then optionally joins ABS Census 2021 postcode-level data.

Functions
---------
parse_dwelling_type(address_series)
    Classifies each address as apartment or house via regex.

load_bom_climate_zones()
    Returns a dict {postcode_str: climate_zone_int} for NSW/ACT.

load_abs_enrichment(abs_csv_path)
    Reads a user-supplied ABS Postal Areas CSV and returns a
    postcode-keyed DataFrame of census features.

enrich_sites(csv_path, abs_csv_path=None)
    Orchestrates all of the above.  Returns a DataFrame with one row
    per SiteUID containing all available feature columns.
"""

import glob
import re
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Paths — auto-detected relative to this file's location
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
_ABS_DATA_DIR = _HERE / "abs_data"
_ATO_CSV = _HERE / "ts23individual25countaveragemedianbypostcode.csv"


# ---------------------------------------------------------------------------
# 1.  Address → dwelling type
# ---------------------------------------------------------------------------

_APARTMENT_PATTERN = re.compile(
    r"""
    \b(
        unit   | apt    | apartment | level  | suite  | flat   |
        shop   | lot    | studio    | office | floor  | tower  |
        u\s*\d | u/\d
    )\b
    | ^[\d]+[a-z]?/          # e.g.  5/12 Smith St
    | /\s*\d+\s+             # e.g.  Unit 3 / 12 ...
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_dwelling_type(address_series: pd.Series) -> pd.Series:
    """
    Return a boolean Series: True = apartment / unit, False = house.

    Parameters
    ----------
    address_series : pd.Series of raw address strings (may contain NaN)
    """
    cleaned = address_series.fillna("").astype(str).str.strip()
    return cleaned.apply(lambda a: bool(_APARTMENT_PATTERN.search(a)))


# ---------------------------------------------------------------------------
# 2.  Postcode → BOM climate zone  (NSW / ACT)
# ---------------------------------------------------------------------------
# BOM zones: 1=Hot humid, 2=Warm humid, 3=Hot dry, 4=Mixed dry,
#            5=Warm temperate, 6=Mild temperate, 7=Cool temperate, 8=Alpine
#
# Source: NCC 2022 Climate Zone map, cross-referenced with ABS postcode
# boundaries for NSW/ACT.  Only postcodes that appear in the Reposit fleet
# are listed; unknown postcodes default to zone 5 (Sydney temperate).

_BOM_ZONES: dict[str, int] = {
    # ── ACT ──────────────────────────────────────────────────────────────
    "2600": 7, "2601": 7, "2602": 7, "2603": 7, "2604": 7,
    "2605": 7, "2606": 7, "2607": 7, "2608": 7, "2609": 7,
    "2610": 7, "2611": 7, "2612": 7, "2613": 7, "2614": 7,
    "2615": 7, "2616": 7, "2617": 7, "2618": 6, "2619": 6,
    "2620": 6, "2900": 7, "2901": 7, "2902": 7, "2903": 7,
    "2904": 7, "2905": 7, "2906": 7, "2911": 7, "2912": 7,
    "2913": 7, "2914": 7,
    # ── Greater Sydney (Zone 5) ──────────────────────────────────────────
    "2000": 5, "2001": 5, "2006": 5, "2007": 5, "2008": 5,
    "2009": 5, "2010": 5, "2011": 5, "2015": 5, "2016": 5,
    "2017": 5, "2018": 5, "2019": 5, "2020": 5, "2021": 5,
    "2022": 5, "2023": 5, "2024": 5, "2025": 5, "2026": 5,
    "2027": 5, "2028": 5, "2029": 5, "2030": 5, "2031": 5,
    "2032": 5, "2033": 5, "2034": 5, "2035": 5, "2036": 5,
    "2037": 5, "2038": 5, "2039": 5, "2040": 5, "2041": 5,
    "2042": 5, "2043": 5, "2044": 5, "2045": 5, "2046": 5,
    "2047": 5, "2048": 5, "2049": 5, "2050": 5, "2060": 5,
    "2061": 5, "2062": 5, "2063": 5, "2064": 5, "2065": 5,
    "2066": 5, "2067": 5, "2068": 5, "2069": 5, "2070": 5,
    "2071": 5, "2072": 5, "2073": 5, "2074": 5, "2075": 5,
    "2076": 5, "2077": 5, "2079": 5, "2080": 6, "2081": 6,
    "2082": 6, "2083": 6, "2084": 5, "2085": 5, "2086": 5,
    "2087": 5, "2088": 5, "2089": 5, "2090": 5, "2092": 5,
    "2093": 5, "2094": 5, "2095": 5, "2096": 5, "2097": 5,
    "2099": 5, "2100": 5, "2101": 5, "2102": 5, "2103": 5,
    "2104": 5, "2105": 5, "2106": 5, "2107": 5, "2108": 5,
    "2110": 5, "2111": 5, "2112": 5, "2113": 5, "2114": 5,
    "2115": 5, "2116": 5, "2117": 5, "2118": 5, "2119": 5,
    "2120": 5, "2121": 5, "2122": 5, "2125": 5, "2126": 5,
    "2127": 5, "2128": 5, "2130": 5, "2131": 5, "2132": 5,
    "2133": 5, "2134": 5, "2135": 5, "2136": 5, "2137": 5,
    "2138": 5, "2140": 5, "2141": 5, "2142": 5, "2143": 5,
    "2144": 5, "2145": 5, "2146": 5, "2147": 5, "2148": 5,
    "2150": 5, "2151": 5, "2152": 5, "2153": 5, "2154": 5,
    "2155": 5, "2156": 5, "2157": 5, "2158": 5, "2159": 5,
    "2160": 5, "2161": 5, "2162": 5, "2163": 5, "2164": 5,
    "2165": 5, "2166": 5, "2167": 5, "2168": 5, "2170": 5,
    "2171": 5, "2172": 5, "2173": 5, "2174": 5, "2175": 5,
    "2176": 5, "2177": 5, "2178": 5, "2179": 5, "2190": 5,
    "2191": 5, "2192": 5, "2193": 5, "2194": 5, "2195": 5,
    "2196": 5, "2197": 5, "2198": 5, "2199": 5, "2200": 5,
    "2203": 5, "2204": 5, "2205": 5, "2206": 5, "2207": 5,
    "2208": 5, "2209": 5, "2210": 5, "2211": 5, "2212": 5,
    "2213": 5, "2214": 5, "2216": 5, "2217": 5, "2218": 5,
    "2219": 5, "2220": 5, "2221": 5, "2222": 5, "2223": 5,
    "2224": 5, "2225": 5, "2226": 5, "2227": 5, "2228": 5,
    "2229": 5, "2230": 5, "2231": 5, "2232": 5, "2233": 5,
    "2234": 5, "2250": 5, "2251": 5, "2256": 5, "2257": 5,
    "2258": 5, "2259": 5, "2260": 5, "2261": 5, "2262": 5,
    "2263": 5, "2264": 5, "2265": 5, "2267": 5, "2278": 5,
    "2280": 5, "2281": 5, "2282": 5, "2283": 5, "2284": 5,
    "2285": 5, "2286": 5, "2287": 5, "2289": 5, "2290": 5,
    "2291": 5, "2292": 5, "2293": 5, "2294": 5, "2295": 5,
    "2296": 5, "2297": 5, "2298": 5, "2299": 5, "2300": 5,
    "2302": 5, "2303": 5, "2304": 5, "2305": 5, "2306": 5,
    "2307": 5, "2308": 5, "2315": 5, "2316": 5, "2317": 5,
    "2318": 5, "2319": 5, "2320": 5, "2321": 5, "2322": 5,
    "2323": 5, "2324": 5, "2325": 5, "2326": 5, "2327": 5,
    "2328": 4, "2329": 4, "2330": 4, "2331": 4, "2333": 4,
    "2334": 4, "2335": 4,
    # ── Blue Mountains / Southern Highlands (Zone 6/7) ───────────────────
    "2745": 6, "2747": 6, "2748": 6, "2749": 6, "2750": 6,
    "2752": 6, "2753": 6, "2754": 6, "2756": 6, "2757": 6,
    "2758": 6, "2759": 5, "2760": 5, "2761": 5, "2762": 5,
    "2763": 5, "2765": 5, "2766": 5, "2767": 5, "2768": 5,
    "2769": 5, "2770": 5, "2773": 7, "2774": 6, "2775": 6,
    "2776": 7, "2777": 6, "2778": 7, "2779": 7, "2780": 7,
    "2782": 7, "2783": 7, "2784": 7, "2785": 7, "2786": 7,
    "2787": 7, "2790": 6,
    # ── Wollongong / Illawarra (Zone 5) ─────────────────────────────────
    "2500": 5, "2502": 5, "2505": 5, "2506": 5, "2508": 5,
    "2515": 5, "2516": 5, "2517": 5, "2518": 5, "2519": 5,
    "2520": 5, "2521": 5, "2522": 5, "2523": 5, "2524": 5,
    "2525": 5, "2526": 5, "2527": 5, "2528": 5, "2529": 5,
    "2530": 5, "2533": 5, "2534": 5, "2535": 5, "2536": 5,
    "2537": 5, "2538": 5, "2539": 5, "2540": 5, "2541": 5,
    "2545": 5,
    # ── Southern Highlands / Tablelands (Zone 6/7) ──────────────────────
    "2550": 5, "2551": 5, "2575": 6, "2576": 6, "2577": 6,
    "2578": 6, "2579": 6, "2580": 6, "2581": 6, "2582": 6,
    "2583": 6, "2584": 6, "2585": 6, "2586": 6, "2587": 6,
    "2588": 6, "2590": 6, "2594": 6,
    # ── Central West / Inland NSW (Zone 4) ──────────────────────────────
    "2800": 4, "2803": 4, "2804": 4, "2805": 4, "2806": 4,
    "2807": 4, "2808": 4, "2809": 4, "2810": 4, "2820": 4,
    "2821": 4, "2822": 4, "2823": 4, "2824": 4, "2825": 4,
    "2826": 4, "2827": 4, "2828": 4, "2829": 4, "2830": 4,
    "2831": 4, "2832": 4, "2833": 4, "2834": 4, "2835": 4,
    "2836": 4, "2838": 4, "2839": 4, "2840": 4, "2842": 4,
    "2843": 4, "2844": 4, "2845": 4, "2846": 4, "2847": 4,
    "2848": 4, "2849": 4, "2850": 4, "2852": 4,
    # ── Riverina / Wagga (Zone 4) ────────────────────────────────────────
    "2640": 4, "2641": 4, "2642": 4, "2643": 4, "2644": 4,
    "2645": 4, "2646": 4, "2647": 4, "2648": 3, "2649": 4,
    "2650": 4, "2651": 4, "2652": 4, "2653": 4, "2655": 4,
    "2656": 4, "2657": 4, "2658": 4, "2659": 4, "2660": 4,
    "2661": 4, "2663": 4, "2665": 4, "2666": 4, "2668": 4,
    "2669": 4, "2671": 4, "2672": 4, "2675": 3, "2678": 4,
    "2680": 4, "2681": 4, "2700": 4, "2701": 4, "2702": 4,
    "2703": 4, "2705": 4, "2706": 4, "2707": 4, "2708": 4,
    "2710": 4, "2711": 4, "2712": 4, "2713": 4, "2714": 4,
    "2715": 3, "2716": 3, "2717": 3,
    # ── Far West NSW (Zone 3) ─────────────────────────────────────────────
    "2830": 4, "2840": 4, "2880": 3, "2881": 3, "2882": 3,
    "2883": 3, "2884": 3, "2885": 3, "2886": 3, "2887": 3,
    "2888": 3, "2889": 3, "2890": 3,
    # ── Northern NSW coast (Zone 2) ──────────────────────────────────────
    "2350": 5, "2351": 5, "2352": 5, "2354": 5, "2355": 5,
    "2356": 4, "2357": 4, "2358": 4, "2359": 4, "2360": 4,
    "2361": 4, "2365": 4, "2369": 4, "2370": 4, "2371": 4,
    "2372": 4, "2380": 4, "2381": 4, "2382": 4, "2386": 4,
    "2387": 4, "2388": 4, "2390": 4, "2395": 4, "2396": 4,
    "2397": 4, "2399": 4, "2400": 4, "2401": 4, "2402": 4,
    "2403": 4, "2404": 4, "2405": 4, "2406": 3, "2408": 3,
    "2409": 3, "2410": 3, "2411": 3, "2415": 5, "2420": 5,
    "2421": 5, "2422": 5, "2423": 5, "2424": 5, "2425": 5,
    "2426": 5, "2427": 5, "2428": 5, "2429": 5, "2430": 5,
    "2431": 2, "2439": 2, "2440": 2, "2441": 2, "2443": 2,
    "2444": 2, "2445": 2, "2446": 2, "2447": 2, "2448": 2,
    "2449": 2, "2450": 2, "2452": 2, "2453": 2, "2454": 2,
    "2455": 2, "2456": 2, "2460": 2, "2461": 2, "2462": 2,
    "2463": 2, "2464": 2, "2465": 2, "2466": 2, "2469": 2,
    "2470": 2, "2471": 2, "2472": 2, "2473": 2, "2474": 2,
    "2475": 2, "2476": 2, "2477": 2, "2478": 2, "2479": 2,
    "2480": 2, "2481": 2, "2482": 2, "2483": 2, "2484": 2,
    "2485": 2, "2486": 2, "2487": 2, "2488": 2, "2489": 2,
    "2490": 2,
}

_DEFAULT_CLIMATE_ZONE = 5  # Sydney temperate — fallback for unknown postcodes

_CLIMATE_ZONE_LABELS = {
    1: "Hot humid summer",
    2: "Warm humid",
    3: "Hot dry",
    4: "Mixed dry",
    5: "Warm temperate",
    6: "Mild temperate",
    7: "Cool temperate",
    8: "Alpine",
}


def load_bom_climate_zones() -> dict[str, int]:
    """Return the embedded postcode → BOM climate zone dict."""
    return dict(_BOM_ZONES)


def climate_zone_label(zone: int) -> str:
    return _CLIMATE_ZONE_LABELS.get(zone, f"Zone {zone}")


# ---------------------------------------------------------------------------
# 3.  ABS Census 2021 enrichment  (optional, user-supplied CSV)
# ---------------------------------------------------------------------------

# Expected column names in the user's ABS Postal Areas CSV.
# The user downloads the 2021 Census "Postal Areas" datapack from abs.gov.au
# and exports the relevant table to CSV.  Column names may vary slightly;
# we accept common variants via the alias map below.

_ABS_COLUMN_ALIASES: dict[str, list[str]] = {
    "postcode": [
        "POA_CODE_2021", "POA_CODE21", "POA_CODE", "Postcode",
        "postcode", "POA", "poa_code",
    ],
    "median_income": [
        "Median_tot_hhd_inc_weekly", "Median_household_income_weekly",
        "median_hhd_income", "Med_tot_hhd_inc_weekly",
    ],
    "pct_dwellings_house": [
        "Dwellings_Separate_house_Percentage", "Sep_house_pct",
        "pct_separate_house", "Separate_house_percent",
    ],
    "pct_wfh": [
        "Employed_worked_from_home_Percentage", "WFH_percent",
        "pct_work_from_home", "Worked_from_home_pct",
    ],
    "seifa_score": [
        "SEIFA_IRSD_Score", "IRSD_Score", "seifa_irsd",
        "SEIFA_score", "Index_of_relative_socio_economic_disadvantage",
    ],
}


def _resolve_column(df: pd.DataFrame, canonical: str) -> str | None:
    """Return the first alias that exists in df.columns, or None."""
    for alias in _ABS_COLUMN_ALIASES.get(canonical, []):
        if alias in df.columns:
            return alias
    return None


def load_abs_enrichment(abs_csv_path: str) -> pd.DataFrame | None:
    """
    Read the user-supplied ABS Postal Areas CSV.

    Returns a DataFrame indexed by postcode string with columns:
        median_income, pct_dwellings_house, pct_wfh, seifa_score
    Returns None if the file cannot be parsed.
    """
    try:
        abs_df = pd.read_csv(abs_csv_path, dtype=str, low_memory=False)
    except Exception as exc:
        print(f"      [enrichment] Could not read ABS CSV: {exc}")
        return None

    pc_col = _resolve_column(abs_df, "postcode")
    if pc_col is None:
        print("      [enrichment] ABS CSV missing postcode column — skipping.")
        return None

    result = pd.DataFrame()
    result["postcode"] = abs_df[pc_col].astype(str).str.strip().str.zfill(4)

    for canonical in ("median_income", "pct_dwellings_house", "pct_wfh", "seifa_score"):
        col = _resolve_column(abs_df, canonical)
        if col:
            result[canonical] = pd.to_numeric(abs_df[col], errors="coerce")
        else:
            print(f"      [enrichment] ABS column '{canonical}' not found — will be skipped.")

    result = result.set_index("postcode")
    print(f"      [enrichment] ABS enrichment loaded: {len(result)} postcodes, "
          f"columns: {list(result.columns)}")
    return result


# ---------------------------------------------------------------------------
# 4.  Master enrichment function
# ---------------------------------------------------------------------------

def _find_abs_files(table: str) -> list[Path]:
    """
    Glob for ABS Census GCP Postal Area CSV files for a given table code
    (e.g. 'G02', 'G04A', 'G04B', 'G43', 'G34') across all state sub-folders
    inside abs_data/.
    """
    pattern = str(_ABS_DATA_DIR / "**" / f"2021Census_{table}_*_POA.csv")
    return [Path(p) for p in glob.glob(pattern, recursive=True)]


def _read_abs_table(table: str) -> pd.DataFrame | None:
    """
    Read and concatenate all state-level ABS POA CSVs for a given table.
    Returns a DataFrame indexed by 4-digit postcode string, or None.
    """
    files = _find_abs_files(table)
    if not files:
        return None
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str, low_memory=False)
            frames.append(df)
        except Exception as exc:
            print(f"      [enrichment] Could not read {f.name}: {exc}")
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    combined["POA_CODE_2021"] = combined["POA_CODE_2021"].astype(str).str.strip().str.zfill(4)
    combined = combined.drop_duplicates(subset="POA_CODE_2021").set_index("POA_CODE_2021")
    return combined


def load_abs_census_data() -> pd.DataFrame | None:
    """
    Build a postcode-keyed DataFrame of ABS Census 2021 demographic features
    from the G02, G04B, and G43 tables extracted under abs_data/.

    Computed features
    -----------------
    median_household_income  — G02  Median_tot_hhd_inc_weekly
    avg_household_size       — G02  Average_household_size
    avg_persons_per_bedroom  — G02  Average_num_psns_per_bedroom
    median_weekly_rent       — G02  Median_rent_weekly
    pct_65_plus              — G04B  sum(65–100+) / Tot_P
    pct_not_in_labour_force  — G43  lfs_N_the_labour_force_P / P_15_yrs_over_P
    pct_part_time_employed   — G43  lfs_Emplyed_wrked_part_time_P / P_15_yrs_over_P
    pct_university_educated  — G43  (PostGrad + GradDip + Bachelor) / P_15_yrs_over_P
    """
    if not _ABS_DATA_DIR.exists():
        print("      [enrichment] abs_data/ folder not found — ABS census features skipped.")
        return None

    result = pd.DataFrame()

    # ── G02: summary medians and averages ────────────────────────────────
    g02 = _read_abs_table("G02")
    if g02 is not None:
        for col, out in [
            ("Median_tot_hhd_inc_weekly",    "median_household_income"),
            ("Average_household_size",        "avg_household_size"),
            ("Average_num_psns_per_bedroom",  "avg_persons_per_bedroom"),
            ("Median_rent_weekly",            "median_weekly_rent"),
        ]:
            if col in g02.columns:
                result[out] = pd.to_numeric(g02[col], errors="coerce")
        print(f"      [enrichment] G02 loaded: {len(g02)} postcodes.")
    else:
        print("      [enrichment] G02 not found — median/average features skipped.")

    # ── G04B: retirement-age population ──────────────────────────────────
    g04b = _read_abs_table("G04B")
    if g04b is not None:
        numeric = g04b.apply(pd.to_numeric, errors="coerce")
        bands_65_plus = [
            "Age_yr_65_69_P", "Age_yr_70_74_P", "Age_yr_75_79_P",
            "Age_yr_80_84_P", "Age_yr_85_89_P", "Age_yr_90_94_P",
            "Age_yr_95_99_P", "Age_yr_100_yr_over_P",
        ]
        available_65 = [c for c in bands_65_plus if c in numeric.columns]
        tot_col = "Tot_P" if "Tot_P" in numeric.columns else None
        if available_65 and tot_col:
            sum_65 = numeric[available_65].sum(axis=1)
            tot = numeric[tot_col].replace(0, np.nan)
            result["pct_65_plus"] = (sum_65 / tot * 100).round(2)
        print(f"      [enrichment] G04B loaded: {len(g04b)} postcodes.")

    # ── G43: labour force status + education ─────────────────────────────
    g43 = _read_abs_table("G43")
    if g43 is not None:
        numeric43 = g43.apply(pd.to_numeric, errors="coerce")
        pop15 = numeric43.get("P_15_yrs_over_P", pd.Series(dtype=float)).replace(0, np.nan)

        nilf = numeric43.get("lfs_N_the_labour_force_P")
        if nilf is not None:
            result["pct_not_in_labour_force"] = (
                nilf.reindex(result.index) / pop15.reindex(result.index) * 100
            ).round(2)

        part_time = numeric43.get("lfs_Emplyed_wrked_part_time_P")
        if part_time is not None:
            result["pct_part_time_employed"] = (
                part_time.reindex(result.index) / pop15.reindex(result.index) * 100
            ).round(2)

        uni_cols = [
            "non_sch_qual_PostGrad_Dgre_P",
            "non_sch_qual_Gr_Dip_Gr_Crt_P",
            "non_sch_qual_Bchelr_Degree_P",
        ]
        available_uni = [c for c in uni_cols if c in numeric43.columns]
        if available_uni:
            sum_uni = numeric43[available_uni].sum(axis=1)
            result["pct_university_educated"] = (
                sum_uni.reindex(result.index) / pop15.reindex(result.index) * 100
            ).round(2)

        print(f"      [enrichment] G43 loaded: {len(g43)} postcodes.")

    if result.empty:
        return None

    print(f"      [enrichment] ABS Census features: {list(result.columns)} across {len(result)} postcodes.")
    return result


def load_ato_data() -> pd.DataFrame | None:
    """
    Load ATO Taxation Statistics 2022-23 Table 25 (income by postcode).

    Returns a postcode-indexed DataFrame with:
        avg_taxable_income    — Average taxable income or loss
        median_taxable_income — Median taxable income or loss
    """
    if not _ATO_CSV.exists():
        print("      [enrichment] ATO CSV not found — income features skipped.")
        return None

    try:
        df = pd.read_csv(_ATO_CSV, dtype=str, low_memory=False)
    except Exception as exc:
        print(f"      [enrichment] Could not read ATO CSV: {exc}")
        return None

    # Normalise postcode column
    pc_col = next((c for c in df.columns if "postcode" in c.lower()), None)
    if pc_col is None:
        print("      [enrichment] ATO CSV has no Postcode column — skipped.")
        return None

    df["_pc"] = df[pc_col].astype(str).str.strip().str.zfill(4)
    df = df.drop_duplicates(subset="_pc").set_index("_pc")

    result = pd.DataFrame(index=df.index)

    avg_col = next((c for c in df.columns if "average taxable" in c.lower()), None)
    med_col = next((c for c in df.columns if "median taxable" in c.lower()), None)

    if avg_col:
        result["avg_taxable_income"] = pd.to_numeric(df[avg_col], errors="coerce")
    if med_col:
        result["median_taxable_income"] = pd.to_numeric(df[med_col], errors="coerce")

    print(f"      [enrichment] ATO data loaded: {len(result)} postcodes, columns: {list(result.columns)}")
    return result


def enrich_sites(csv_path: str, abs_csv_path: str | None = None) -> pd.DataFrame:
    """
    Build a per-SiteUID feature DataFrame from the main energy CSV.

    Always derives:
        is_apartment    — bool
        climate_zone    — int 1–8
        climate_label   — str description
        Solar_kW        — float (NaN if column absent)
        Battery_kWh     — float (NaN if column absent)

    Additionally joins (if ABS CSV supplied):
        median_income, pct_dwellings_house, pct_wfh, seifa_score

    Parameters
    ----------
    csv_path      : path to the main energy CSV
    abs_csv_path  : path to user-supplied ABS Postal Areas CSV (optional)

    Returns
    -------
    pd.DataFrame  indexed by SiteUID string
    """
    print("[enrich] Loading site metadata from CSV …")

    header = pd.read_csv(csv_path, nrows=0)
    available_cols = set(header.columns)

    load_cols = ["SiteUID"]
    for col in ("Address", "Postcode", "Solar_kW", "Battery_kWh"):
        if col in available_cols:
            load_cols.append(col)

    if len(load_cols) == 1:
        print("      [enrichment] No metadata columns found — enrichment skipped.")
        return pd.DataFrame()

    # Read one row per SiteUID (first occurrence carries the metadata).
    raw = pd.read_csv(csv_path, usecols=load_cols, dtype=str, low_memory=False)
    sites = (
        raw.drop_duplicates(subset="SiteUID")
        .set_index("SiteUID")
    )

    enriched = pd.DataFrame(index=sites.index)

    # ── Dwelling type ────────────────────────────────────────────────────
    if "Address" in sites.columns:
        enriched["is_apartment"] = parse_dwelling_type(sites["Address"]).astype(int)
        n_apt = enriched["is_apartment"].sum()
        print(f"      [enrichment] {n_apt}/{len(enriched)} sites classified as apartments.")
    else:
        enriched["is_apartment"] = np.nan

    # ── Climate zone ─────────────────────────────────────────────────────
    zones = load_bom_climate_zones()
    if "Postcode" in sites.columns:
        pc = sites["Postcode"].astype(str).str.strip().str.zfill(4)
        enriched["climate_zone"] = pc.map(zones).fillna(_DEFAULT_CLIMATE_ZONE).astype(int)
        enriched["climate_label"] = enriched["climate_zone"].map(climate_zone_label)
        unknown = pc.map(zones).isna().sum()
        if unknown:
            print(f"      [enrichment] {unknown} postcodes not in BOM table — defaulted to zone {_DEFAULT_CLIMATE_ZONE}.")
    else:
        enriched["climate_zone"] = _DEFAULT_CLIMATE_ZONE
        enriched["climate_label"] = climate_zone_label(_DEFAULT_CLIMATE_ZONE)

    # ── Solar / battery ───────────────────────────────────────────────────
    for col in ("Solar_kW", "Battery_kWh"):
        if col in sites.columns:
            enriched[col] = pd.to_numeric(sites[col], errors="coerce")
        else:
            enriched[col] = np.nan

    # ── ABS Census data (auto-detected from abs_data/) ────────────────────
    abs_census = load_abs_census_data()
    if abs_census is not None and "Postcode" in sites.columns:
        pc_series = sites["Postcode"].astype(str).str.strip().str.zfill(4)
        for col in abs_census.columns:
            enriched[col] = pc_series.map(abs_census[col])
        matched = pc_series.isin(abs_census.index).sum()
        print(f"      [enrichment] ABS Census joined: {matched}/{len(enriched)} sites matched.")

    # ── Optional user-supplied ABS CSV (legacy / override) ───────────────
    if abs_csv_path:
        abs_df = load_abs_enrichment(abs_csv_path)
        if abs_df is not None and "Postcode" in sites.columns:
            pc_series = sites["Postcode"].astype(str).str.strip().str.zfill(4)
            for col in abs_df.columns:
                enriched[col] = pc_series.map(abs_df[col])
            matched = pc_series.map(abs_df.iloc[:, 0]).notna().sum()
            print(f"      [enrichment] User ABS CSV joined: {matched}/{len(enriched)} sites matched.")

    print(f"      [enrichment] Enriched DataFrame: {len(enriched)} sites, "
          f"features: {[c for c in enriched.columns if c != 'climate_label']}")
    return enriched
