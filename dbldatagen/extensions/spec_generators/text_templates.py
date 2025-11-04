from __future__ import annotations
import re
import difflib
from typing import Dict, Optional, Tuple, Sequence
from rapidfuzz import process, fuzz

_model = None
_RX_NON_ALNUM = re.compile(r"[^a-z0-9]+")

COLUMN_TEMPLATES = {
    # Personal Information
    "first_name": "\\w",
    "last_name": "\\w",
    "surname": "\\w",
    "full_name": "\\w \\w",
    "username": "kkk_kkk",
    "email": "\\w.\\w@\\w.com|\\w@\\w.co.u\\k",
    "email_alt": "\\w@\\w.com",
    "password_hash": "xxxxxxxxxxxxxxxx",
    "phone_number": "(ddd)-ddd-dddd",
    "mobile_number": "ddd-ddd-dddd",
    "home_phone": "ddd-ddd-dddd",
    "dob": "yyyy-mm-dd",
    "gender": "M|F|O",
    "ssn": "ddd-dd-dddd",
    "passport_number": "wwdddddd",
    "drivers_license": "wwdddddd",
    "national_id": "dddddddddd",
    "marital_status": "Single|Married|Divorced|Widowed",
    "age": "dd",
    "title": "Mr|Ms|Mrs|Dr|Prof",
    "middle_name": "\\w",
    "suffix": "Jr|Sr|II|III|IV",
    "nickname": "\\w",
    "birthplace": "\\w",
    "citizenship": "\\w",
    "ethnicity": "\\w",
    "language": "\\w",
    "religion": "\\w",
    # Address Information
    "street_address": "n \\w \\w",
    "address_line1": "n \\w \\w",
    "address_line2": "Apt n|Unit n",
    "city": "\\w",
    "state": "\\w\\w",
    "province": "\\w",
    "region": "\\w",
    "zip_code": "ddddd",
    "postal_code": "ddddd",
    "country": "\\w",
    "country_code": "ww",
    "timezone": "www/www",
    # Business/Employment
    "company_name": "\\w \\w",
    "business_name": "\\w \\w",
    "department": "\\w",
    "job_title": "\\w \\w",
    "employee_id": "EMPddddd",
    "manager_id": "EMPddddd",
    "hire_date": "yyyy-mm-dd",
    "termination_date": "yyyy-mm-dd",
    "salary": "dddddd",
    "bonus": "ddd",
    "office_phone": "ddd-ddd-dddd",
    "work_email": "\\w.\\w@\\w.com",
    "work_location": "\\w",
    "supervisor": "\\w \\w",
    "team": "\\w",
    "shift": "Morning|Evening|Night",
    # Financial
    "credit_card": "dddd-dddd-dddd-dddd",
    "credit_card_expiry": "mm/yy",
    "credit_card_cvv": "ddd",
    "account_number": "dddddddddd",
    "routing_number": "dddddddd",
    "iban": "wwdddddddddddddddddddddd",
    "swift_code": "wwwwwwww",
    "tax_id": "dd-dddddddd",
    "bank_name": "\\w \\w",
    "balance": "dddddd",
    "currency": "www",
    # Capital Markets
    "ticker": "WWW|WWWW|WWWWW",
    "isin": "WWdddddddddd",
    "cusip": "Wdddddddd",
    "sedol": "dWWWWdd",
    "figi": "WWWWWWWWWWWWWW",
    "ric": "WWWW.WW",
    "exchange_code": "WWWW",
    "security_id": "SECdddddd",
    "instrument_id": "INSdddddd",
    "asset_class": "Equity|Bond|FX|Commodity|Derivative",
    "security_type": "Stock|Bond|Option|Future|ETF|ADR|GDR",
    "market_cap": "dddddddddd",
    "shares_outstanding": "dddddddddd",
    "price": "ddd.dd",
    "volume": "dddddddd",
    "bid_price": "ddd.dd",
    "ask_price": "ddd.dd",
    "last_trade_price": "ddd.dd",
    "open_price": "ddd.dd",
    "close_price": "ddd.dd",
    "high_price": "ddd.dd",
    "low_price": "ddd.dd",
    "trade_id": "TRDdddddd",
    "order_id": "ORDdddddd",
    "execution_id": "EXCdddddd",
    "portfolio_id": "PRTdddddd",
    "fund_id": "FNDdddddd",
    "index_id": "IDXdddddd",
    "option_id": "OPTdddddd",
    "future_id": "FUTdddddd",
    "bond_id": "BNDdddddd",
    "etf_id": "ETFdddddd",
    "mutual_fund_id": "MFdddddd",
    # Product/Inventory
    "product_code": "PRDddddd",
    "sku": "SKUddddd",
    "upc": "dddddddddddd",
    "ean": "ddddddddddddd",
    "isbn": "d-ddd-ddddd-d",
    "model_number": "MDLddddd",
    "serial_number": "SNdddddddd",
    # System/Network
    "ip_address": "n.n.n.n",
    "mac_address": "xx:xx:xx:xx:xx:xx",
    "hostname": "kkkkkk",
    "domain": "\\w.com|\\w.net|\\w.org",
    "url": "https://\\w.\\w.com/\\w",
    "api_key": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "session_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "token": "xxxxxxxxxxxxxxxx",
    "device_id": "DEVddddd",
    "machine_id": "MCHddddd",
    "server_id": "SVRddddd",
    "cluster_id": "CLSddddd",
    "node_id": "NODddddd",
    # Document/File
    "document_id": "DOCddddd",
    "file_name": "\\w.\\w",
    "file_path": "/\\w/\\w/\\w.\\w",
    "mime_type": "\\w/\\w",
    "file_extension": ".\\w",
    # Location/Geo
    "latitude": "dd.dddddd",
    "longitude": "ddd.dddddd",
    "gps_coordinates": "dd.dddddd, ddd.dddddd",
    # Social Media
    "twitter_handle": "@\\w",
    "instagram_username": "@\\w",
    "facebook_id": "dddddddddd",
    "linkedin_id": "\\w-\\w-\\w",
    # Miscellaneous
    "status": "Active|Inactive|Pending|Closed",
    "priority": "High|Medium|Low",
    "category": "\\w",
    "type": "\\w",
    "description": "\\w \\w \\w",
    "notes": "\\w \\w \\w",
    "comments": "\\w \\w \\w",
    "reference": "REFddddd",
    "code": "dddddd",
    "flag": "Y|N",
    "is_active": "true|false",
    "created_at": "yyyy-mm-dd hh:mm:ss",
    "updated_at": "yyyy-mm-dd hh:mm:ss",
    "timestamp": "yyyy-mm-dd hh:mm:ss",
}


# def _semantic_match(name: str, candidates: Tuple[str, ...]) -> Tuple[str, float]:
#     """Transformer‑based semantic similarity (0‑1)."""
#     if not _model:
#         return "", 0.0
#     emb_query = _model.encode(name, normalize_embeddings=True)
#     emb_cand = _model.encode(list(candidates), normalize_embeddings=True)
#     sims = util.cos_sim(emb_query, emb_cand)[0].tolist()
#     idx = int(max(range(len(sims)), key=sims.__getitem__))
#     return candidates[idx], sims[idx]


def _normalize(s: str) -> str:
    """Lower‑case, strip accents (if any), collapse to ASCII letters+digits."""
    s = s.lower()
    s = _RX_NON_ALNUM.sub("_", s)
    return re.sub(r"_{2,}", "_", s).strip("_")


def _fuzzy_match(name: str, candidates: Sequence[str]) -> Tuple[str, float]:
    """
    Return the best fuzzy match among *candidates* for *name* and the score
    on a 0‑1 scale.  If nothing passes, returns ("", 0.0).
    """
    # ---- RapidFuzz branch --------------------------------------------------
    try:
        result = process.extractOne(
            name,
            list(candidates),  # cast to list for the stub
            scorer=fuzz.token_sort_ratio,  # kw‑only param -> fine
        )
        if result is not None:
            best, score, _ = result  # result = (match, score, idx)
            return best, score / 100.0
    except ImportError:
        pass

    # ---- difflib fallback --------------------------------------------------
    best = difflib.get_close_matches(name, candidates, n=1, cutoff=0)
    if best:
        score = difflib.SequenceMatcher(None, name, best[0]).ratio()
        return best[0], score

    return "", 0.0


def guess_template(col_name: str) -> Optional[str]:
    """
    Given an *original* column name from a database, return the most relevant
    synthetic‑data template (regex / string) from ``column_templates``.
    If no reasonable match is found, return ``None``.

    Advanced matching stack:
    1.  Exact key      → O(1)
    2.  Alias map      → O(1)
    3.  Fuzzy match    → rapidfuzz or difflib
    4.  Semantic match → sentence‑transformers
    """
    if not col_name:
        return None
    norm = _normalize(col_name)
    # exact match
    if norm in COLUMN_TEMPLATES:
        return COLUMN_TEMPLATES[norm]

    # # alias table
    # if norm in ALIASES:
    #     return column_templates[ALIASES[norm]]

    # candidate keys to consider in later steps
    keys = tuple(COLUMN_TEMPLATES.keys())

    # fuzzy distance
    best_fuzzy, fuzzy_score = _fuzzy_match(norm, keys)
    if fuzzy_score >= 0.9:  # very high confidence
        return COLUMN_TEMPLATES[best_fuzzy]

    # # TODO: semantic similarity
    # best_sem, sem_score = _semantic_match(col_name, keys)
    # if sem_score >= 0.6 and sem_score > fuzzy_score:
    #     return COLUMN_TEMPLATES[best_sem]

    # lower‑threshold fuzzy fallback
    if fuzzy_score >= 0.75:
        return COLUMN_TEMPLATES[best_fuzzy]

    # Nothing good enough
    return None
