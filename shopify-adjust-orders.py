import os
import re
import time
import csv
import datetime
import logging
from datetime import timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import requests
from dotenv import load_dotenv


# ----------------------------
# FORCE-LOAD .env FROM THIS SCRIPT'S FOLDER (VS CODE SAFE)
# ----------------------------
ENV_PATH = Path(__file__).resolve().parent / ".env"
loaded = load_dotenv(dotenv_path=ENV_PATH, override=True)
print("Loaded .env:", loaded, "from", str(ENV_PATH))


# ----------------------------
# ENV HELPERS
# ----------------------------
def env_first(*names: str, default: Optional[str] = None) -> Optional[str]:
    for name in names:
        value = os.getenv(name)
        if value is not None and str(value).strip() != "":
            return value.strip()
    return default


def env_bool(*names: str, default: bool = False) -> bool:
    value = env_first(*names)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(*names: str, default: int) -> int:
    value = env_first(*names)
    if value is None:
        return default
    return int(str(value).strip())


def parse_draft_order_names(raw: Optional[str]) -> List[str]:
    if not raw:
        return []

    text = str(raw).strip()
    if text in {"[]", '[""]', "['']"}:
        return []

    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].strip()

    if not text:
        return []

    parts = [x.strip().strip('"').strip("'") for x in text.split(",")]
    return [x for x in parts if x]


def parse_csv_set(raw: Optional[str], *, casefold: bool = False) -> Set[str]:
    if not raw:
        return set()
    vals = []
    for part in str(raw).split(","):
        v = part.strip()
        if not v:
            continue
        vals.append(v.casefold() if casefold else v)
    return set(vals)


# ----------------------------
# ENV CONFIG
# ----------------------------
SHOP = env_first("SHOPIFY_SHOP", "SHOPIFY_STORE")
TOKEN = env_first("SHOPIFY_ADMIN_ACCESS_TOKEN", "SHOPIFY_TOKEN")
API_VERSION = env_first("SHOPIFY_API_VERSION", "API_VERSION", default="2025-07")
LOCATION_ID = env_first("SHOPIFY_LOCATION_ID", "LOCATION_ID")

DRAFT_ORDER_NAMES = parse_draft_order_names(env_first("DRAFT_ORDER_NAMES"))

DRY_RUN = env_bool("DRY_RUN", default=True)
MAX_DRAFTS = env_int("MAX_DRAFTS", default=250)
LOOKBACK_DAYS = env_int("LOOKBACK_DAYS", default=3)
LOG_LEVEL = (env_first("LOG_LEVEL", default="INFO") or "INFO").upper()

PO_SUFFIX_FORMAT = env_first("PO_SUFFIX_FORMAT", default=" - BO{bucket}") or " - BO{bucket}"
IDEMPOTENCY_DONE_TAG = env_first("IDEMPOTENCY_DONE_TAG", default="split-backorder-done") or "split-backorder-done"
PROCESSING_TAG = env_first("PROCESSING_TAG", default="split-backorder-processing") or "split-backorder-processing"
CHILD_TAG = env_first("CHILD_TAG", default="split-backorder-child") or "split-backorder-child"

EXCLUDED_CUSTOMERS = parse_csv_set(env_first("EXCLUDED_CUSTOMERS", default=""), casefold=True)
CLEAR_STALE_PROCESSING_TAGS = env_bool("CLEAR_STALE_PROCESSING_TAGS", default=True)

# Linking fields
LINK_CUSTOM_ATTR_PO_KEY = env_first("LINK_CUSTOM_ATTR_PO_KEY", default="original_poNumber") or "original_poNumber"
LINK_CUSTOM_ATTR_DRAFTID_KEY = env_first("LINK_CUSTOM_ATTR_DRAFTID_KEY", default="original_draft_id") or "original_draft_id"

LINK_METAFIELD_NAMESPACE = env_first("LINK_METAFIELD_NAMESPACE", default="lifelines") or "lifelines"
LINK_METAFIELD_KEY = env_first("LINK_METAFIELD_KEY", default="original_po_number") or "original_po_number"
LINK_METAFIELD_TYPE = env_first("LINK_METAFIELD_TYPE", default="single_line_text_field") or "single_line_text_field"

PO_METAFIELD_NAMESPACE = env_first("PO_METAFIELD_NAMESPACE", default="b2b") or "b2b"
PO_METAFIELD_KEY = env_first("PO_METAFIELD_KEY", default="po_number") or "po_number"
PO_METAFIELD_TYPE = env_first("PO_METAFIELD_TYPE", default="single_line_text_field") or "single_line_text_field"

SHIP_DATE_METAFIELD_NAMESPACE = env_first("SHIP_DATE_METAFIELD_NAMESPACE", default="b2b") or "b2b"
SHIP_DATE_METAFIELD_KEY = env_first("SHIP_DATE_METAFIELD_KEY", default="ship_date") or "ship_date"

ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE = (
    env_first("ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE", default="custom") or "custom"
)
ORIGINAL_DRAFT_ID_METAFIELD_KEY = (
    env_first("ORIGINAL_DRAFT_ID_METAFIELD_KEY", default="original_draft_id") or "original_draft_id"
)
ORIGINAL_DRAFT_ID_METAFIELD_TYPE = (
    env_first("ORIGINAL_DRAFT_ID_METAFIELD_TYPE", default="single_line_text_field") or "single_line_text_field"
)

PAYMENT_TERMS_TEMPLATE_ID_FALLBACK = env_first("PAYMENT_TERMS_TEMPLATE_ID", default="") or ""
SET_PAYMENT_TERMS_ON_CHILDREN = env_bool("SET_PAYMENT_TERMS_ON_CHILDREN", default=True)

print("SHOPIFY_SHOP =", SHOP)
print("API_VERSION  =", API_VERSION)
print("DRAFT_ORDER_NAMES =", DRAFT_ORDER_NAMES)
print("LOCATION_ID =", LOCATION_ID)
print("DRY_RUN =", DRY_RUN)
print("LOOKBACK_DAYS =", LOOKBACK_DAYS)
print("PROCESSING_TAG =", PROCESSING_TAG)
print("EXCLUDED_CUSTOMERS =", sorted(EXCLUDED_CUSTOMERS))
print("CLEAR_STALE_PROCESSING_TAGS =", CLEAR_STALE_PROCESSING_TAGS)

if not SHOP or not TOKEN:
    raise SystemExit(
        "Missing shop/token env vars. Accepted names:\n"
        "  SHOPIFY_SHOP or SHOPIFY_STORE\n"
        "  SHOPIFY_ADMIN_ACCESS_TOKEN or SHOPIFY_TOKEN"
    )
if not LOCATION_ID:
    raise SystemExit(
        "Missing location env var. Accepted names:\n"
        "  SHOPIFY_LOCATION_ID or LOCATION_ID"
    )

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("shopify-draft-order-splitter")


def normalize_draft_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip()
    s = s.replace("Draft", "").strip()
    if s.startswith("#"):
        s = s[1:]
    return s.strip().upper()


def normalize_customer_name(name: str) -> str:
    return (name or "").strip().casefold()


def build_draft_name_query(names: List[str]) -> str:
    vals: List[str] = []
    seen = set()
    for n in names:
        raw = str(n).strip()
        if not raw:
            continue
        base = raw.lstrip("#").strip()
        candidates = [raw, base, f"#{base}"]
        for c in candidates:
            c = c.strip()
            if not c:
                continue
            key = c.lower()
            if key in seen:
                continue
            seen.add(key)
            vals.append(c)

    parts = []
    for v in vals:
        parts.append(f'name:"{v}"')
        if "#" not in v:
            parts.append(f"name:{v}")
    return " OR ".join(parts)


# ----------------------------
# TAG → BUCKET MAP
# ----------------------------
def build_tag_bucket_map() -> Dict[str, int]:
    mapping: Dict[str, int] = {}

    bucket_keys: List[Tuple[int, str]] = []
    for k in os.environ.keys():
        m = re.fullmatch(r"PRODUCT_TAG_BUCKET_(\d+)", k.strip())
        if m:
            bucket_keys.append((int(m.group(1)), k))

    if not bucket_keys:
        bucket_keys = [(i, f"PRODUCT_TAG_BUCKET_{i}") for i in (1, 2, 3, 4)]

    for bucket_num, env_key in sorted(bucket_keys, key=lambda x: x[0]):
        raw = (os.getenv(env_key) or "").strip()
        if not raw:
            continue
        tags = [t.strip() for t in raw.split(",") if t.strip()]
        for tag in tags:
            mapping[tag] = bucket_num

    return mapping


TAG_BUCKET_MAP = build_tag_bucket_map()
MAX_BUCKET = max([1, *TAG_BUCKET_MAP.values()])

LOG_CSV_PATH = (env_first("LOG_CSV_PATH", default="split_drafts_log.csv") or "split_drafts_log.csv").strip()
LOG_MAX_BUCKET_ENV = (env_first("LOG_MAX_BUCKET", default="") or "").strip()
LOG_MAX_BUCKET = int(LOG_MAX_BUCKET_ENV) if LOG_MAX_BUCKET_ENV else max(10, MAX_BUCKET)

print("Loaded TAG_BUCKET_MAP:", TAG_BUCKET_MAP)
print("Max bucket:", MAX_BUCKET)
print("Idempotency tag:", IDEMPOTENCY_DONE_TAG)


# ----------------------------
# PO NUMBER HANDLING
# ----------------------------
def build_po_number(original_po: Optional[str], bucket: int) -> str:
    base = (original_po or "").strip()
    suffix = PO_SUFFIX_FORMAT.format(bucket=bucket)

    if not base:
        return f"BACKORDER-{bucket}"

    if base.endswith(suffix):
        return base

    return base + suffix


# ----------------------------
# GRAPHQL HELPER
# ----------------------------
def gql(query: str, variables: Optional[Dict[str, Any]] = None, *, attempts: int = 5) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": TOKEN,
    }

    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                headers=headers,
                json={"query": query, "variables": variables or {}},
                timeout=60,
            )
            if resp.status_code in (429, 503):
                sleep_s = min(2 ** i, 10)
                logger.warning("Throttled (HTTP %s). Sleeping %ss and retrying...", resp.status_code, sleep_s)
                time.sleep(sleep_s)
                continue

            if resp.status_code != 200:
                raise RuntimeError(
                    f"HTTP {resp.status_code} calling Shopify GraphQL. URL={GRAPHQL_URL}\nResponse:\n{resp.text}"
                )

            data = resp.json()
            if "errors" in data and data["errors"]:
                raise RuntimeError(f"GraphQL errors:\n{data['errors']}")
            return data["data"]
        except Exception as e:
            last_err = e
            sleep_s = min(2 ** i, 10)
            logger.warning("GraphQL call failed (attempt %s/%s): %s", i + 1, attempts, e)
            if i < attempts - 1:
                time.sleep(sleep_s)
    raise RuntimeError(f"GraphQL call failed after {attempts} attempts: {last_err}")


# ----------------------------
# QUERIES / MUTATIONS
# ----------------------------
QUERY_DRAFTS = """
query($first:Int!, $after:String, $query:String) {
  draftOrders(first:$first, after:$after, query:$query, reverse:true) {
    edges { cursor node { id name tags } }
    pageInfo { hasNextPage endCursor }
  }
}
"""

QUERY_DRAFT_DETAIL = """
query($id:ID!, $locationId:ID!, $poNamespace: String!, $poKey: String!, $shipDateNamespace: String!, $shipDateKey: String!) {
  draftOrder(id:$id) {
    id
    name
    poNumber
    email
    shippingAddress { company name }
    billingAddress { company name }
    tags
    note2
    presentmentCurrencyCode
    paymentTerms {
      dueInDays
      paymentTermsName
      paymentTermsType
    }

    customAttributes { key value }

    po_meta: metafield(namespace: $poNamespace, key: $poKey) { value }
    ship_date_meta: metafield(namespace: $shipDateNamespace, key: $shipDateKey) { value }

    metafields(first:250) {
      nodes {
        namespace
        key
        type
        value
      }
    }

    lineItems(first:250) {
      nodes {
        quantity
        title

        appliedDiscount {
          description
          title
          value
          valueType
          amountV2 { amount currencyCode }
        }

        originalUnitPriceWithCurrency { amount currencyCode }
        priceOverride { amount currencyCode }

        variant {
          id
          product { tags title }
          inventoryItem {
            tracked
            inventoryLevel(locationId:$locationId) {
              quantities(names:["available"]) { name quantity }
            }
          }
        }
      }
    }
  }
}
"""

MUTATION_DUPLICATE = """
mutation($id: ID!) {
  draftOrderDuplicate(id: $id) {
    draftOrder { id name }
    userErrors { field message }
  }
}
"""

MUTATION_UPDATE = """
mutation($id:ID!, $input:DraftOrderInput!) {
  draftOrderUpdate(id:$id, input:$input) {
    draftOrder {
      id
      name
      tags
      poNumber
      lineItems(first: 250) {
        edges { node { id } }
      }
    }
    userErrors { message field }
  }
}
"""

MUTATION_DELETE = """
mutation($id:ID!) {
  draftOrderDelete(input:{id:$id}) {
    deletedId
    userErrors { field message }
  }
}
"""

QUERY_FIND_CHILD = """
query($first:Int!, $after:String, $query:String, $ns:String!, $key:String!) {
  draftOrders(first:$first, after:$after, query:$query, reverse:true) {
    edges {
      cursor
      node {
        id
        name
        tags
        link: metafield(namespace:$ns, key:$key) { value }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


# ----------------------------
# Helpers
# ----------------------------
def money_input(m: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not m:
        return None
    amt = m.get("amount")
    if amt is None:
        return None
    out = {"amount": str(amt)}
    if m.get("currencyCode"):
        out["currencyCode"] = m["currencyCode"]
    return out


def applied_discount_input(ad: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not ad:
        return None
    out: Dict[str, Any] = {
        "description": ad.get("description"),
        "title": ad.get("title"),
        "value": ad.get("value"),
        "valueType": ad.get("valueType"),
    }
    if ad.get("amountV2") and ad["amountV2"].get("amount") is not None:
        out["amount"] = str(ad["amountV2"]["amount"])
    return {k: v for k, v in out.items() if v is not None} or None


def merge_custom_attributes(existing: List[Dict[str, Any]], additions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, str] = {}
    for item in existing or []:
        k = item.get("key")
        v = item.get("value")
        if k:
            merged[str(k)] = "" if v is None else str(v)
    for item in additions or []:
        k = item.get("key")
        v = item.get("value")
        if k:
            merged[str(k)] = "" if v is None else str(v)
    return [{"key": k, "value": v} for k, v in merged.items()]


def merge_metafields(existing: List[Dict[str, Any]], additions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for item in existing or []:
        ns = (item.get("namespace") or "").strip()
        key = (item.get("key") or "").strip()
        if not ns or not key:
            continue

        mf_type = (item.get("type") or "").strip()
        value = item.get("value")
        if value is None:
            continue

        merged[(ns, key)] = {
            "namespace": ns,
            "key": key,
            "type": mf_type,
            "value": str(value),
        }

    for item in additions or []:
        ns = (item.get("namespace") or "").strip()
        key = (item.get("key") or "").strip()
        if not ns or not key:
            continue

        mf_type = (item.get("type") or "").strip()
        value = item.get("value")
        if value is None:
            continue

        merged[(ns, key)] = {
            "namespace": ns,
            "key": key,
            "type": mf_type,
            "value": str(value),
        }

    return list(merged.values())


def parse_ship_date_value(raw: Optional[str]) -> Optional[datetime.date]:
    if not raw:
        return None

    text = str(raw).strip()
    if not text:
        return None

    try:
        if "T" in text:
            iso_text = text.replace("Z", "+00:00")
            return datetime.datetime.fromisoformat(iso_text).date()
        return datetime.date.fromisoformat(text)
    except Exception:
        return None


def ship_date_is_eligible(raw_ship_date: Optional[str]) -> Tuple[bool, Optional[datetime.date], datetime.date]:
    ship_date = parse_ship_date_value(raw_ship_date)
    today = datetime.datetime.now().date()
    tomorrow = today + datetime.timedelta(days=1)

    if raw_ship_date is None or str(raw_ship_date).strip() == "":
        return True, None, tomorrow

    if ship_date is None:
        return False, None, tomorrow

    return ship_date < tomorrow, ship_date, tomorrow


def build_linking_fields(
    *,
    base_po: str,
    original_draft_id: str,
    is_child: bool,
    bucket: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    base_po = (base_po or "").strip()
    ca_add = [
        {"key": LINK_CUSTOM_ATTR_PO_KEY, "value": base_po},
        {"key": LINK_CUSTOM_ATTR_DRAFTID_KEY, "value": original_draft_id},
    ]

    mf_add: List[Dict[str, Any]] = [
        {
            "namespace": ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE,
            "key": ORIGINAL_DRAFT_ID_METAFIELD_KEY,
            "type": ORIGINAL_DRAFT_ID_METAFIELD_TYPE,
            "value": original_draft_id,
        }
    ]

    if is_child and base_po and bucket:
        child_po = build_po_number(base_po, bucket)
        mf_add.append(
            {
                "namespace": PO_METAFIELD_NAMESPACE,
                "key": PO_METAFIELD_KEY,
                "type": PO_METAFIELD_TYPE,
                "value": child_po,
            }
        )

    return ca_add, mf_add


# ----------------------------
# INVENTORY CHECK
# ----------------------------
def get_available_qty(line: Dict[str, Any]) -> Optional[int]:
    try:
        variant = line.get("variant") or {}
        inv_item = variant.get("inventoryItem") or {}
        tracked = inv_item.get("tracked")
        if tracked is False:
            return None

        level = inv_item.get("inventoryLevel")
        if not level:
            return 0

        quantities = level.get("quantities") or []
        for q in quantities:
            if q.get("name") == "available":
                return int(q.get("quantity") or 0)

        return 0
    except Exception:
        return None


# ----------------------------
# RULE ENGINE
# ----------------------------
def decide_bucket(line: Dict[str, Any]) -> Optional[int]:
    variant = line.get("variant")
    qty = int(line.get("quantity") or 0)

    if not variant:
        return None

    tags = set((variant.get("product") or {}).get("tags") or [])

    for tag, bucket in sorted(TAG_BUCKET_MAP.items(), key=lambda x: x[1]):
        if tag in tags:
            return bucket

    available = get_available_qty(line)
    if available is not None and available < qty:
        return 1

    return None


# ----------------------------
# BUILD LINE INPUT
# ----------------------------
def build_line_input(line: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"quantity": int(line.get("quantity") or 0)}

    if line.get("variant"):
        out["variantId"] = line["variant"]["id"]

        po = money_input(line.get("priceOverride"))
        if po:
            out["priceOverride"] = po
        else:
            oup = money_input(line.get("originalUnitPriceWithCurrency"))
            if oup:
                out["priceOverride"] = oup
    else:
        out["title"] = line.get("title") or "Custom item"
        oup = money_input(line.get("originalUnitPriceWithCurrency"))
        if oup:
            out["originalUnitPriceWithCurrency"] = oup

    lad = applied_discount_input(line.get("appliedDiscount"))
    if lad:
        out["appliedDiscount"] = lad

    return {k: v for k, v in out.items() if v is not None}


def get_lineitems_total_count(draft_node: Dict[str, Any]) -> Optional[int]:
    try:
        li = draft_node.get("lineItems") or {}
        edges = li.get("edges") or []
        return len(edges)
    except Exception:
        return None


# ----------------------------
# MUTATION WRAPPERS
# ----------------------------
def draft_duplicate(original_id: str) -> Dict[str, Any]:
    if DRY_RUN:
        return {"id": "DRY_RUN_DUPLICATE", "name": "DRY_RUN_DUPLICATE"}

    res = gql(MUTATION_DUPLICATE, {"id": original_id})["draftOrderDuplicate"]
    errs = res.get("userErrors") or []
    if errs:
        raise RuntimeError(f"draftOrderDuplicate userErrors: {errs}")
    d = res.get("draftOrder")
    if not d:
        raise RuntimeError("draftOrderDuplicate returned no draftOrder")
    return d


def draft_update_return(draft_id: str, input_data: Dict[str, Any], label: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if DRY_RUN:
        logger.info("DRY RUN — would update %s: %s", label, draft_id)
        return [], {}
    res = gql(MUTATION_UPDATE, {"id": draft_id, "input": input_data})["draftOrderUpdate"]
    errs = res.get("userErrors") or []
    d = res.get("draftOrder") or {}
    if not errs:
        logger.info("Updated %s: %s | poNumber=%s", label, d.get("name"), d.get("poNumber"))
    return errs, d


def draft_delete(draft_id: str, label: str) -> None:
    if DRY_RUN:
        logger.info("DRY RUN — would delete %s: %s", label, draft_id)
        return

    try:
        res = gql(MUTATION_DELETE, {"id": draft_id})["draftOrderDelete"]
        errs = res.get("userErrors") or []
        if errs:
            logger.warning("draftOrderDelete userErrors (%s): %s", label, errs)
        else:
            logger.info("Deleted %s: %s", label, draft_id)
    except Exception as e:
        logger.warning("Failed to delete %s %s: %s", label, draft_id, e)


# ----------------------------
# CHILD LOOKUP
# ----------------------------
def find_existing_child(original_draft_id: str, bucket: int) -> Optional[Dict[str, str]]:
    q = f'tag:"Backorder #{bucket}" tag:{CHILD_TAG} status:open'
    after = None
    while True:
        resp = gql(
            QUERY_FIND_CHILD,
            {
                "first": 50,
                "after": after,
                "query": q,
                "ns": ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE,
                "key": ORIGINAL_DRAFT_ID_METAFIELD_KEY,
            },
        ).get("draftOrders") or {}

        edges = resp.get("edges") or []
        for e in edges:
            node = e.get("node") or {}
            link = node.get("link") or {}
            if (link.get("value") or "") == original_draft_id:
                return {"id": node.get("id", ""), "name": node.get("name", "")}

        page = resp.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        after = page.get("endCursor")
        if not after:
            break
    return None


# ----------------------------
# LOG HELPERS
# ----------------------------
def customer_label_for_log(draft: Dict[str, Any]) -> str:
    for addr_key in ("shippingAddress", "billingAddress"):
        addr = draft.get(addr_key) or {}
        company = (addr.get("company") or "").strip()
        name = (addr.get("name") or "").strip()
        if company:
            return company
        if name:
            return name
    email = (draft.get("email") or "").strip()
    return email


def upsert_split_log_row(
    *,
    csv_path: Path,
    row: Dict[str, str],
    key_field: str = "original_draft_id",
) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows: List[Dict[str, str]] = []
    headers: List[str] = []

    if csv_path.exists():
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            existing_rows = list(reader)

    def ensure_header(h: str) -> None:
        if h not in headers:
            headers.append(h)

    for k in row.keys():
        ensure_header(k)

    key_val = row.get(key_field, "")
    replaced = False
    for i, r in enumerate(existing_rows):
        if (r.get(key_field) or "") == key_val and key_val:
            existing_rows[i] = {**r, **row}
            replaced = True
            break
    if not replaced:
        existing_rows.append(row)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(existing_rows)


# ----------------------------
# PAYMENT TERMS HELPERS
# ----------------------------
_VALID_NET_DAYS = {30, 60, 90, 120}
_NET_PATTERNS = [
    re.compile(r"\bnet\s*[-:]*\s*(30|60|90|120)\b", re.IGNORECASE),
    re.compile(r"\bnet(30|60|90|120)\b", re.IGNORECASE),
    re.compile(r"\bterms?\s*[:\-]?\s*net\s*(30|60|90|120)\b", re.IGNORECASE),
    re.compile(r"\bn\s*(30|60|90|120)\b", re.IGNORECASE),
]


def infer_net_days_from_note(note_text: str) -> int:
    if not note_text:
        return 0
    found = set()
    for pat in _NET_PATTERNS:
        for m in pat.finditer(note_text):
            try:
                d = int(m.group(1))
            except Exception:
                continue
            if d in _VALID_NET_DAYS:
                found.add(d)
    return next(iter(found)) if len(found) == 1 else 0


def template_id_for_net_days(days: int) -> str:
    if not days:
        return ""
    return (os.getenv(f"PAYMENT_TERMS_TEMPLATE_ID_NET{days}") or "").strip()


def payment_terms_template_id_from_draft(draft: Dict[str, Any]) -> str:
    try:
        pt = draft.get("paymentTerms") or {}
        days = pt.get("dueInDays")
        if days is None:
            return ""
        days = int(days)
        return template_id_for_net_days(days) or ""
    except Exception:
        return ""


def build_payment_terms_input(template_id: str) -> Optional[Dict[str, Any]]:
    if not template_id:
        return None
    return {"paymentTermsTemplateId": template_id}


def build_payment_terms_input_with_issue_date(template_id: str) -> Optional[Dict[str, Any]]:
    if not template_id:
        return None
    issued_at = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    return {
        "paymentTermsTemplateId": template_id,
        "paymentSchedules": {"issuedAt": issued_at},
    }


def apply_update_with_retries(
    *,
    draft_id: str,
    input_data: Dict[str, Any],
    label: str,
    terms_template_id: str = "",
) -> Dict[str, Any]:
    errs, d_last = draft_update_return(draft_id, input_data, label)
    if not errs:
        return d_last

    msg_join = " | ".join((e.get("message", "") or "") for e in errs).lower()

    if "issue date is required" in msg_join and terms_template_id:
        pti2 = build_payment_terms_input_with_issue_date(terms_template_id)
        if pti2:
            input_data = dict(input_data)
            input_data["paymentTerms"] = pti2
        errs2, d2 = draft_update_return(
            draft_id, input_data, label=f"{label} (retry with issuedAt)"
        )
        if not errs2:
            return d2
        errs = errs2
        msg_join = " | ".join((e.get("message", "") or "") for e in errs).lower()

    if "cannot add shipping when no line items require shipping" in msg_join:
        input_data = dict(input_data)
        input_data["shippingLine"] = None
        errs3, d3 = draft_update_return(
            draft_id, input_data, label=f"{label} (retry without shippingLine)"
        )
        if not errs3:
            return d3
        errs = errs3

    raise RuntimeError(f"draftOrderUpdate userErrors ({label}): {errs}")


# ----------------------------
# TAG / LOCK HELPERS
# ----------------------------
def with_tag(tags: List[str], tag: str) -> List[str]:
    out = list(tags or [])
    if tag not in out:
        out.append(tag)
    return out


def without_tag(tags: List[str], tag: str) -> List[str]:
    return [t for t in (tags or []) if t != tag]


def claim_processing_lock(draft: Dict[str, Any]) -> bool:
    tags = list(draft.get("tags") or [])

    if CHILD_TAG in tags:
        return False
    if IDEMPOTENCY_DONE_TAG in tags:
        return False
    if PROCESSING_TAG in tags:
        return False

    if DRY_RUN:
        logger.info("DRY RUN — would add processing tag to %s", draft.get("name"))
        return True

    new_tags = with_tag(tags, PROCESSING_TAG)
    errs, updated = draft_update_return(
        draft["id"],
        {"tags": new_tags},
        label="claim processing lock",
    )
    if errs:
        raise RuntimeError(f"Failed to claim processing lock: {errs}")

    updated_tags = set(updated.get("tags") or [])
    return PROCESSING_TAG in updated_tags


def release_processing_lock(draft_id: str, tags: List[str]) -> None:
    if DRY_RUN:
        logger.info("DRY RUN — would remove processing tag from %s", draft_id)
        return

    cleaned = without_tag(tags, PROCESSING_TAG)
    errs, _ = draft_update_return(
        draft_id,
        {"tags": cleaned},
        label="release processing lock",
    )
    if errs:
        logger.warning("Failed to release processing lock for %s: %s", draft_id, errs)


# ----------------------------
# DRAFT PROCESSOR
# ----------------------------
def process_draft(draft_id: str) -> str:
    draft = gql(
        QUERY_DRAFT_DETAIL,
        {
            "id": draft_id,
            "locationId": LOCATION_ID,
            "poNamespace": PO_METAFIELD_NAMESPACE,
            "poKey": PO_METAFIELD_KEY,
            "shipDateNamespace": SHIP_DATE_METAFIELD_NAMESPACE,
            "shipDateKey": SHIP_DATE_METAFIELD_KEY,
        },
    )["draftOrder"]

    if not draft:
        logger.info("Draft not found: %s", draft_id)
        return "skipped"

    raw_ship_date = ((draft.get("ship_date_meta") or {}).get("value") or "").strip()
    ship_ok, parsed_ship_date, tomorrow = ship_date_is_eligible(raw_ship_date)

    if not ship_ok:
        if raw_ship_date:
            logger.info(
                "%s: SKIP (b2b.ship_date=%r is invalid or not earlier than %s).",
                draft.get("name"),
                raw_ship_date,
                tomorrow.isoformat(),
            )
        else:
            logger.info(
                "%s: SKIP (invalid b2b.ship_date metafield).",
                draft.get("name"),
            )
        return "skipped"

    if DRAFT_ORDER_NAMES:
        targets = {normalize_draft_name(n) for n in DRAFT_ORDER_NAMES}
        if normalize_draft_name(draft.get("name", "")) not in targets:
            logger.info("%s: SKIP (not in DRAFT_ORDER_NAMES)", draft.get("name"))
            return "skipped"

    customer_name = customer_label_for_log(draft).strip()
    customer_name_norm = normalize_customer_name(customer_name)
    if customer_name_norm and customer_name_norm in EXCLUDED_CUSTOMERS:
        logger.info("%s: SKIP (excluded customer: %s)", draft.get("name"), customer_name)
        return "skipped"

    existing_tags = list(draft.get("tags") or [])

    if "needs-review" in existing_tags:
        logger.info("%s: SKIP (tag 'needs-review' present).", draft.get("name"))
        return "skipped"

    if CHILD_TAG in existing_tags:
        logger.info("%s: SKIP (is a split child; tag '%s' present).", draft.get("name"), CHILD_TAG)
        return "skipped"

    if IDEMPOTENCY_DONE_TAG in existing_tags:
        logger.info("%s: SKIP (already processed; tag '%s' present).", draft["name"], IDEMPOTENCY_DONE_TAG)
        return "skipped"

    if PROCESSING_TAG in existing_tags:
        if CLEAR_STALE_PROCESSING_TAGS:
            logger.info(
                "%s: stale processing tag found; clearing '%s' and continuing.",
                draft["name"],
                PROCESSING_TAG,
            )
            release_processing_lock(draft_id, existing_tags)

            draft = gql(
                QUERY_DRAFT_DETAIL,
                {
                    "id": draft_id,
                    "locationId": LOCATION_ID,
                    "poNamespace": PO_METAFIELD_NAMESPACE,
                    "poKey": PO_METAFIELD_KEY,
                    "shipDateNamespace": SHIP_DATE_METAFIELD_NAMESPACE,
                    "shipDateKey": SHIP_DATE_METAFIELD_KEY,
                },
            )["draftOrder"]

            if not draft:
                logger.info("Draft not found after clearing stale lock: %s", draft_id)
                return "skipped"

            existing_tags = list(draft.get("tags") or [])
            if "needs-review" in existing_tags:
                logger.info("%s: SKIP (tag 'needs-review' present after refresh).", draft.get("name"))
                return "skipped"
            if PROCESSING_TAG in existing_tags:
                logger.info("%s: SKIP (processing tag still present after clear attempt).", draft["name"])
                return "skipped"
        else:
            logger.info("%s: SKIP (already being processed; tag '%s' present).", draft["name"], PROCESSING_TAG)
            return "skipped"

    lock_claimed = claim_processing_lock(draft)
    if not lock_claimed:
        logger.info("%s: SKIP (could not claim processing lock).", draft["name"])
        return "skipped"

    draft = gql(
        QUERY_DRAFT_DETAIL,
        {
            "id": draft_id,
            "locationId": LOCATION_ID,
            "poNamespace": PO_METAFIELD_NAMESPACE,
            "poKey": PO_METAFIELD_KEY,
            "shipDateNamespace": SHIP_DATE_METAFIELD_NAMESPACE,
            "shipDateKey": SHIP_DATE_METAFIELD_KEY,
        },
    )["draftOrder"]

    existing_tags = list(draft.get("tags") or [])

    if "needs-review" in existing_tags:
        logger.info("%s: SKIP (tag 'needs-review' present after lock/reload).", draft.get("name"))
        release_processing_lock(draft_id, existing_tags)
        return "skipped"

    lines = (draft.get("lineItems") or {}).get("nodes") or []
    original_full_line_items_input = [build_line_input(l) for l in lines]

    buckets: Dict[int, List[Dict[str, Any]]] = {b: [] for b in range(1, MAX_BUCKET + 1)}
    keep: List[Dict[str, Any]] = []

    for line in lines:
        bucket = decide_bucket(line)
        if bucket:
            buckets[bucket].append(line)
        else:
            keep.append(line)

    if all(len(v) == 0 for v in buckets.values()):
        logger.info("%s: no backorders needed.", draft["name"])
        release_processing_lock(draft_id, existing_tags)
        return "skipped"

    primary_bucket_for_original: Optional[int] = None
    if not keep:
        non_empty = [b for b, ls in buckets.items() if ls]
        primary_bucket_for_original = min(non_empty) if non_empty else None
        if primary_bucket_for_original is not None:
            keep = buckets[primary_bucket_for_original]
            buckets[primary_bucket_for_original] = []

    if not keep:
        logger.error("%s: original would have 0 line items after split. Skipping.", draft["name"])
        release_processing_lock(draft_id, existing_tags)
        return "skipped"

    existing_po_meta = (draft.get("po_meta") or {}).get("value")
    base_po = (existing_po_meta or draft.get("poNumber") or "").strip()
    original_po = base_po

    note_text = draft.get("note2") or ""
    net_days = infer_net_days_from_note(note_text)
    note_terms_template_id = template_id_for_net_days(net_days)
    original_terms_template_id = (
        payment_terms_template_id_from_draft(draft)
        or note_terms_template_id
        or PAYMENT_TERMS_TEMPLATE_ID_FALLBACK
    )

    if original_terms_template_id:
        logger.info("Payment terms template id: %s", original_terms_template_id)
    else:
        logger.info("Payment terms template id: (none)")

    original_name = draft.get("name")
    original_tags = list(draft.get("tags") or [])
    original_custom_attributes = draft.get("customAttributes") or []
    original_metafields = ((draft.get("metafields") or {}).get("nodes") or [])

    logger.info("")
    logger.info("Processing %s (DRY_RUN=%s)", original_name, DRY_RUN)
    logger.info("Original PO: %r", original_po)
    if primary_bucket_for_original is None:
        logger.info("Original keeps ship-now lines: %s", len(keep))
    else:
        logger.info("Original assigned to bucket #%s: %s", primary_bucket_for_original, len(keep))

    ca_add_orig, mf_add_orig = build_linking_fields(
        base_po=base_po,
        original_draft_id=draft_id,
        is_child=False,
    )

    bo_draft_ids: Dict[int, str] = {}
    newly_created_child_ids: List[Tuple[int, str]] = []

    try:
        for bucket, bucket_lines in buckets.items():
            if not bucket_lines:
                continue

            existing_child = None if DRY_RUN else find_existing_child(draft_id, bucket)
            created_new = False

            if existing_child:
                dup_id = existing_child["id"]
                dup_name = existing_child.get("name") or ""
                logger.info(
                    "Child bucket #%s: %s line(s) | PO=%s",
                    bucket,
                    len(bucket_lines),
                    build_po_number(original_po, bucket),
                )
                logger.info("Reusing existing child: %s (%s)", dup_name, dup_id)
            else:
                dup = draft_duplicate(draft_id)
                dup_id = dup["id"]
                created_new = not DRY_RUN
                if created_new:
                    newly_created_child_ids.append((bucket, dup_id))
                dup_name = dup.get("name") or ""
                logger.info(
                    "Child bucket #%s: %s line(s) | PO=%s",
                    bucket,
                    len(bucket_lines),
                    build_po_number(original_po, bucket),
                )
                if DRY_RUN:
                    logger.info("DRY RUN — would duplicate original and update duplicate.")
                else:
                    logger.info("Duplicated: %s (%s)", dup_name, dup_id)

            bo_draft_ids[bucket] = f"DRY_RUN_BO{bucket}" if DRY_RUN else dup_id

            new_tags = list(original_tags)
            bucket_tag = f"Backorder #{bucket}"
            if bucket_tag not in new_tags:
                new_tags.append(bucket_tag)
            if CHILD_TAG not in new_tags:
                new_tags.append(CHILD_TAG)

            ca_add_child, mf_add_child = build_linking_fields(
                base_po=base_po,
                original_draft_id=draft_id,
                is_child=True,
                bucket=bucket,
            )

            update_input: Dict[str, Any] = {
                "lineItems": [build_line_input(l) for l in bucket_lines],
                "poNumber": build_po_number(original_po, bucket),
                "tags": new_tags,
                "customAttributes": merge_custom_attributes(original_custom_attributes, ca_add_child),
                "metafields": merge_metafields(original_metafields, mf_add_child),
            }

            if SET_PAYMENT_TERMS_ON_CHILDREN and original_terms_template_id:
                pti = build_payment_terms_input(original_terms_template_id)
                if pti:
                    update_input["paymentTerms"] = pti

            try:
                apply_update_with_retries(
                    draft_id=dup_id,
                    input_data=update_input,
                    label=f"child bucket #{bucket}",
                    terms_template_id=original_terms_template_id,
                )
            except Exception as e:
                logger.error("ERROR updating child bucket #%s: %s", bucket, e)
                for b, did in reversed(newly_created_child_ids):
                    draft_delete(did, label=f"rollback child bucket #{b}")
                raise

        updated_tags = list(original_tags)
        updated_tags = without_tag(updated_tags, PROCESSING_TAG)
        if IDEMPOTENCY_DONE_TAG not in updated_tags:
            updated_tags.append(IDEMPOTENCY_DONE_TAG)
        if primary_bucket_for_original is not None:
            primary_tag = f"Backorder #{primary_bucket_for_original}"
            if primary_tag not in updated_tags:
                updated_tags.append(primary_tag)

        original_update: Dict[str, Any] = {
            "lineItems": [build_line_input(l) for l in keep],
            "tags": updated_tags,
            "customAttributes": merge_custom_attributes(original_custom_attributes, ca_add_orig),
            "metafields": merge_metafields(original_metafields, mf_add_orig),
        }

        logger.info(
            "Updating original: keep %s line(s) + add '%s' + remove '%s'",
            len(keep),
            IDEMPOTENCY_DONE_TAG,
            PROCESSING_TAG,
        )

        if DRY_RUN:
            draft_update_return(draft_id, original_update, label="original")
            return "success"

        updated_node = apply_update_with_retries(
            draft_id=draft_id,
            input_data=original_update,
            label="original",
            terms_template_id=original_terms_template_id,
        )

        v_tags = set(updated_node.get("tags") or [])
        v_total = get_lineitems_total_count(updated_node)
        if IDEMPOTENCY_DONE_TAG not in v_tags or PROCESSING_TAG in v_tags or (
            v_total is not None and v_total != len(keep)
        ):
            raise RuntimeError(
                f"post-update verification failed: "
                f"done_tag_present={IDEMPOTENCY_DONE_TAG in v_tags} "
                f"processing_tag_present={PROCESSING_TAG in v_tags} "
                f"line_count={v_total} expected={len(keep)}"
            )

        logger.info("Verified original updated: %s | lines=%s", updated_node.get("name"), v_total)

        ts = datetime.datetime.now().isoformat(timespec="seconds")
        customer = customer_label_for_log(draft)
        row: Dict[str, str] = {
            "timestamp": ts,
            "po_number": base_po,
            "customer": customer,
            "original_name": original_name or "",
            "original_draft_id": draft_id,
        }
        for b in range(1, LOG_MAX_BUCKET + 1):
            row[f"BO{b}_draft_id"] = bo_draft_ids.get(b, "")

        upsert_split_log_row(
            csv_path=Path(__file__).resolve().parent / LOG_CSV_PATH,
            row=row,
        )

        return "success"

    except Exception:
        for b, did in reversed(newly_created_child_ids):
            draft_delete(did, label=f"rollback child bucket #{b}")

        if not DRY_RUN:
            restore_tags = list(original_tags)
            restore_tags = without_tag(restore_tags, PROCESSING_TAG)
            restore_tags = [t for t in restore_tags if t != IDEMPOTENCY_DONE_TAG]
            restore_input: Dict[str, Any] = {
                "lineItems": original_full_line_items_input,
                "tags": restore_tags,
                "customAttributes": original_custom_attributes,
                "metafields": original_metafields,
            }
            errs_restore, _ = draft_update_return(
                draft_id, restore_input, label="restore original"
            )
            if errs_restore:
                logger.warning("Restore original userErrors: %s", errs_restore)

        raise


# ----------------------------
# MAIN HELPERS
# ----------------------------
def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_open_ended_query() -> str:
    parts = [
        "status:open",
        f"-tag:{IDEMPOTENCY_DONE_TAG}",
        f"-tag:{CHILD_TAG}",
        "-tag:needs-review",
    ]

    if not CLEAR_STALE_PROCESSING_TAGS:
        parts.append(f"-tag:{PROCESSING_TAG}")

    if LOOKBACK_DAYS > 0:
        since = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=LOOKBACK_DAYS)).date().isoformat()
        parts.append(f"updated_at:>={since}")

    return " ".join(parts)


# ----------------------------
# MAIN
# ----------------------------
def main() -> None:
    targets = {normalize_draft_name(n) for n in DRAFT_ORDER_NAMES} if DRAFT_ORDER_NAMES else set()

    collected: List[Dict[str, Any]] = []
    scanned = 0

    if DRAFT_ORDER_NAMES:
        chunk_size = 12
        for chunk in chunk_list(DRAFT_ORDER_NAMES, chunk_size):
            name_query = build_draft_name_query(chunk)
            query = f"status:open ({name_query})" if name_query else "status:open"

            after = None
            while True:
                resp = gql(QUERY_DRAFTS, {"first": 250, "after": after, "query": query}).get("draftOrders") or {}
                edges = resp.get("edges") or []
                if not edges:
                    break

                for e in edges:
                    node = e.get("node") or {}
                    if not node:
                        continue
                    collected.append(node)
                    scanned += 1

                page_info = resp.get("pageInfo") or {}
                after = page_info.get("endCursor")
                if not page_info.get("hasNextPage"):
                    break
    else:
        query = build_open_ended_query()
        page_size = min(250, MAX_DRAFTS)
        after = None

        logger.info("Open-ended query: %s", query)

        while True:
            resp = gql(QUERY_DRAFTS, {"first": page_size, "after": after, "query": query}).get("draftOrders") or {}
            edges = resp.get("edges") or []
            if not edges:
                break

            for e in edges:
                node = e.get("node") or {}
                if not node:
                    continue
                collected.append(node)
                scanned += 1
                if scanned >= MAX_DRAFTS:
                    break

            if scanned >= MAX_DRAFTS:
                break

            page_info = resp.get("pageInfo") or {}
            after = page_info.get("endCursor")
            if not page_info.get("hasNextPage"):
                break

    if not collected:
        logger.info("No drafts found.")
        return

    dedup: Dict[str, Dict[str, Any]] = {}
    for d in collected:
        did = d.get("id")
        if did and did not in dedup:
            dedup[did] = d
    drafts = list(dedup.values())

    if DRAFT_ORDER_NAMES:
        drafts = [d for d in drafts if normalize_draft_name(d.get("name", "")) in targets]

    logger.info(
        "Found %s draft(s) AFTER client-side filter. DRY_RUN=%s (scanned %s draft rows from API)",
        len(drafts),
        DRY_RUN,
        scanned,
    )
    if DRAFT_ORDER_NAMES and not drafts:
        sample = [d.get("name", "") for d in list(dedup.values())[:25]]
        logger.info("Sample of returned drafts: %s", ", ".join(sample))

    successes: List[str] = []
    skipped: List[str] = []
    failed: List[Tuple[str, str]] = []

    for d in drafts:
        draft_name = d.get("name", d.get("id", "(unknown)"))
        try:
            status = process_draft(d["id"])
            if status == "skipped":
                skipped.append(draft_name)
            else:
                successes.append(draft_name)
        except Exception as e:
            failed.append((draft_name, str(e)))
            logger.error("%s: FAILED — %s", draft_name, e)
            continue

    logger.info("")
    logger.info("Run summary")
    logger.info("SUCCESS: %s", len(successes))
    if successes:
        logger.info("  %s", ", ".join(successes))
    logger.info("SKIPPED: %s", len(skipped))
    if skipped:
        logger.info("  %s", ", ".join(skipped))
    logger.info("FAILED: %s", len(failed))
    if failed:
        for draft_name, err in failed:
            logger.info("  %s: %s", draft_name, err)


if __name__ == "__main__":
    main()
