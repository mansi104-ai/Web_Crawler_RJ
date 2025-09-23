"""
utils.py

Small helper utilities for parsing price, area, timestamps, and safe extraction.
"""

import re
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

def ist_now_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %z")

def safe_get_text(el):
    try:
        return el.get_text(strip=True) if el else ""
    except Exception:
        return ""

def parse_area_value_unit(raw_area: str):
    """
    Parse area strings like:
     - "250 sqft" -> (250, "sqft")
     - "1,200 sq.ft." -> (1200, "sqft")
     - "1100 sq ft" -> (1100, "sq ft")
    Returns (float/int or empty string, unit or empty string)
    """
    if not raw_area:
        return ("", "")
    raw = raw_area.strip()
    # common unit tokens
    try:
        # Normalize unicode and remove stray characters
        raw = raw.replace("\u00a0", " ")
        m = re.search(r"([\d,\.]+)\s*([a-zA-Z\. ]+)", raw)
        if m:
            val = m.group(1).replace(",", "")
            unit = m.group(2).strip().replace(".", "")
            # convert to int if possible
            if "." in val:
                num = float(val)
            else:
                num = int(val)
            return (num, unit)
        else:
            # maybe the unit first or only digits
            digits = re.search(r"([\d,\.]+)", raw)
            if digits:
                val = digits.group(1).replace(",", "")
                if "." in val:
                    num = float(val)
                else:
                    num = int(val)
                # try to extract unit by removing digits from string
                unit = re.sub(r"[\d,\.]", "", raw).strip()
                return (num, unit)
    except Exception:
        pass
    return ("", "")

def parse_price_numeric(raw_price: str):
    """
    Best-effort price parsing:
    - "₹ 12,000 /month" => 12000
    - "₹ 15 Lacs" => 1500000 (15 * 100000)
    - "₹ 1.2 Cr" => 12000000
    Returns numeric value or original string if cannot parse (but we prefer numeric)
    """
    if not raw_price:
        return ""
    text = str(raw_price).strip()
    # Remove currency symbols and per-month etc
    text = text.replace("\xa0", " ")
    # Common patterns
    try:
        # if contains 'lakh' or 'lac' or 'L' or 'Cr'
        if re.search(r"(?i)cr|crore", text):
            m = re.search(r"([\d\.,]+)", text)
            if m:
                v = float(m.group(1).replace(",", ""))
                return int(v * 10000000)
        if re.search(r"(?i)lakh|lac|lacs|lakh[s]?", text):
            m = re.search(r"([\d\.,]+)", text)
            if m:
                v = float(m.group(1).replace(",", ""))
                return int(v * 100000)
        # handle short notation like "12,000", "12k", "12K"
        m = re.search(r"([\d,\.]+)\s*(k|K)?", text)
        if m:
            num = m.group(1).replace(",", "")
            if m.group(2) and m.group(2).lower() == "k":
                return int(float(num) * 1000)
            # otherwise return raw digits
            if "." in num:
                return float(num)
            else:
                return int(num)
    except Exception:
        pass
    # fallback: strip non-digit and return numeric
    digits = re.sub(r"[^\d\.]", "", text)
    if digits:
        if "." in digits:
            try:
                return float(digits)
            except Exception:
                return digits
        else:
            try:
                return int(digits)
            except Exception:
                return digits
    return text

def make_numeric_or_none(v):
    if v is None or v == "":
        return ""
    try:
        if isinstance(v, (int, float)):
            return v
        s = str(v)
        if s == "":
            return ""
        s2 = s.replace(",", "")
        if "." in s2:
            return float(s2)
        else:
            return int(s2)
    except Exception:
        return ""
