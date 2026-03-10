

import os
import re
import time
import csv
import datetime
from datetime import timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


# ----------------------------
# FORCE-LOAD .env FROM THIS SCRIPT'S FOLDER (VS CODE SAFE)
# ----------------------------
ENV_PATH = Path(__file__).resolve().parent / ".env"
loaded = load_dotenv(dotenv_path=ENV_PATH, override=True)
print("Loaded .env:", loaded, "from", str(ENV_PATH))

# ----------------------------
# ENV CONFIG
# ----------------------------
SHOP = os.getenv("SHOPIFY_SHOP")
TOKEN = os.getenv("SHOPIFY_ADMIN_ACCESS_TOKEN")
API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2025-07")
LOCATION_ID = os.getenv("SHOPIFY_LOCATION_ID")

DRAFT_ORDER_NAMES = [
    x.strip() for x in (os.getenv("DRAFT_ORDER_NAMES") or "").split(",") if x.strip()
]


def normalize_draft_name(name: str) -> str:
    """Normalize draft order names for matching, e.g. '#D15476' == 'D15476'."""
    if not name:
        return ""
    s = str(name).strip()
    s = s.replace("Draft", "").strip()
    if s.startswith("#"):
        s = s[1:]
    return s.strip().upper()

def build_draft_name_query(names: list[str]) -> str:
    """Build a robust Shopify draftOrders search query for a list of draft names."""
    vals: list[str] = []
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

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MAX_DRAFTS = int(os.getenv("MAX_DRAFTS", "250"))

PO_SUFFIX_FORMAT = os.getenv("PO_SUFFIX_FORMAT", " - BO{bucket}")
IDEMPOTENCY_DONE_TAG = os.getenv("IDEMPOTENCY_DONE_TAG", "split-backorder-done")

# Linking fields (optional but recommended)
LINK_CUSTOM_ATTR_PO_KEY = (os.getenv("LINK_CUSTOM_ATTR_PO_KEY") or "original_poNumber").strip()
LINK_CUSTOM_ATTR_DRAFTID_KEY = (os.getenv("LINK_CUSTOM_ATTR_DRAFTID_KEY") or "original_draft_id").strip()

LINK_METAFIELD_NAMESPACE = (os.getenv("LINK_METAFIELD_NAMESPACE") or "lifelines").strip()
LINK_METAFIELD_KEY = (os.getenv("LINK_METAFIELD_KEY") or "original_po_number").strip()
LINK_METAFIELD_TYPE = (os.getenv("LINK_METAFIELD_TYPE") or "single_line_text_field").strip()

# Preferred metafields for your B2B workflow (override via .env)
# Draft order PO metafield (this is the pinned "PO number" box in Admin if you defined it under Draft orders)
PO_METAFIELD_NAMESPACE = (os.getenv("PO_METAFIELD_NAMESPACE") or "b2b").strip()
PO_METAFIELD_KEY = (os.getenv("PO_METAFIELD_KEY") or "po_number").strip()
PO_METAFIELD_TYPE = (os.getenv("PO_METAFIELD_TYPE") or "single_line_text_field").strip()

# Draft order metafield that links each split draft back to the original draft (single line text)
ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE = (os.getenv("ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE") or "custom").strip()
ORIGINAL_DRAFT_ID_METAFIELD_KEY = (os.getenv("ORIGINAL_DRAFT_ID_METAFIELD_KEY") or "original_draft_id").strip()
ORIGINAL_DRAFT_ID_METAFIELD_TYPE = (os.getenv("ORIGINAL_DRAFT_ID_METAFIELD_TYPE") or "single_line_text_field").strip()

# Payment terms safeguard / porting
# If set, the splitter will set payment terms on children using the original's template id (preferred),
# or fall back to this template id if original has no terms or cannot be read.
PAYMENT_TERMS_TEMPLATE_ID_FALLBACK = (os.getenv("PAYMENT_TERMS_TEMPLATE_ID") or "").strip()
# Set to true to ALWAYS set terms on children when a template id is available (default true).
SET_PAYMENT_TERMS_ON_CHILDREN = (os.getenv("SET_PAYMENT_TERMS_ON_CHILDREN") or "true").lower() == "true"

print("SHOPIFY_SHOP =", SHOP)
print("API_VERSION  =", API_VERSION)
print("DRAFT_ORDER_NAMES =", DRAFT_ORDER_NAMES)
print("LOCATION_ID =", LOCATION_ID)
print("DRY_RUN =", DRY_RUN)

if not SHOP or not TOKEN:
    raise SystemExit("Missing SHOPIFY_SHOP or SHOPIFY_ADMIN_ACCESS_TOKEN in .env")
if not LOCATION_ID:
    raise SystemExit("Missing SHOPIFY_LOCATION_ID in .env")

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"


# ----------------------------
# TAG → BUCKET MAP (from .env)
# ----------------------------
def build_tag_bucket_map() -> Dict[str, int]:
    """
    Supports ANY number of buckets via PRODUCT_TAG_BUCKET_N environment variables.

    Examples:
      PRODUCT_TAG_BUCKET_1=launch-march-2026
      PRODUCT_TAG_BUCKET_2=launch-april-2026
      PRODUCT_TAG_BUCKET_5=launch-july-2026

    Also supports comma-separated tags per bucket:
      PRODUCT_TAG_BUCKET_4=launch-june-2026,launch-july-2026
    """
    mapping: Dict[str, int] = {}

    # Find any PRODUCT_TAG_BUCKET_<number> keys in the environment.
    bucket_keys: List[Tuple[int, str]] = []
    for k in os.environ.keys():
        m = re.fullmatch(r"PRODUCT_TAG_BUCKET_(\d+)", k.strip())
        if m:
            bucket_keys.append((int(m.group(1)), k))

    # Fallback to 1..4 if nothing is set (keeps old behavior).
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
MAX_BUCKET = max([1, *TAG_BUCKET_MAP.values()])  # ensure bucket 1 always exists

# CSV logging config (defaults)
LOG_CSV_PATH = (os.getenv("LOG_CSV_PATH") or "split_drafts_log.csv").strip()
LOG_MAX_BUCKET_ENV = (os.getenv("LOG_MAX_BUCKET") or "").strip()
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
# GRAPHQL HELPER (with light retry on throttling)
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
            if resp.status_code == 429 or resp.status_code == 503:
                # Basic backoff
                sleep_s = min(2 ** i, 10)
                print(f"  Throttled (HTTP {resp.status_code}). Sleeping {sleep_s}s and retrying...")
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
            print(f"  GraphQL call failed (attempt {i+1}/{attempts}): {e}")
            if i < attempts - 1:
                time.sleep(sleep_s)
    raise RuntimeError(f"GraphQL call failed after {attempts} attempts: {last_err}")


# ----------------------------
# QUERIES / MUTATIONS
# ----------------------------
QUERY_DRAFTS = """
query($first:Int!, $after:String, $query:String) {
  draftOrders(first:$first, after:$after, query:$query, reverse:true) {
    edges { cursor node { id name } }
    pageInfo { hasNextPage endCursor }
  }
}
"""

# Keep the draft detail query focused on what we need for splitting + price preservation.
QUERY_DRAFT_DETAIL = """
query($id:ID!, $locationId:ID!, $poNamespace: String!, $poKey: String!) {
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





# Find existing child drafts for an original + bucket (bucket-level idempotency)
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
# Helpers: Money / Discount / CustomAttributes / Metafields
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
    # Keep backward compatible "amount" if present (many stores still accept it).
    if ad.get("amountV2") and ad["amountV2"].get("amount") is not None:
        out["amount"] = str(ad["amountV2"]["amount"])
    return {k: v for k, v in out.items() if v is not None} or None


def merge_custom_attributes(existing: List[Dict[str, Any]], additions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    DraftOrderInput.customAttributes expects list of {key,value}. This merges by key.
    """
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



def build_linking_fields(
    *,
    base_po: str,
    original_draft_id: str,
    is_child: bool,
    bucket: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns (customAttributesAdditions, metafieldsAdditions)

    We always write ORIGINAL_DRAFT_ID metafield so every draft can be traced.
    We write the PO metafield ONLY on children (unique per bucket), leaving the original unchanged.
    """
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
        child_po = f"{base_po}-BO{bucket}"
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
    """
    Returns available inventory at LOCATION_ID for this line's variant.

    Behavior:
    - If inventory tracking is OFF (tracked == False), return None (do not force BO1).
    - If tracking is ON/unknown but inventoryLevel is missing for the location, treat as 0 available.
      (Common when the item has no level at that location.)
    """
    try:
        variant = line.get("variant") or {}
        inv_item = (variant.get("inventoryItem") or {})
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

        # If "available" wasn't returned for some reason, treat as 0 for tracked items.
        return 0
    except Exception:
        return None


# ----------------------------
# RULE ENGINE
# ----------------------------
def decide_bucket(line: Dict[str, Any]) -> Optional[int]:
    """
    Bucket rules (as requested):

    1) If the product has any configured launch tag, assign that bucket.
       - Deterministic: lower bucket numbers win if multiple tags match.

    2) ELSE (no launch tag match):
         If order qty > available qty at LOCATION_ID => bucket 1 (BO1)
         (Inventory tracking off => we do NOT force BO1.)
    """
    variant = line.get("variant")
    qty = int(line.get("quantity") or 0)

    if not variant:
        return None

    tags = set((variant.get("product") or {}).get("tags") or [])

    # Tag buckets first
    for tag, bucket in sorted(TAG_BUCKET_MAP.items(), key=lambda x: x[1]):
        if tag in tags:
            return bucket

    # Only if NO tag bucket applies, use inventory shortfall fallback.
    available = get_available_qty(line)
    if available is not None and available < qty:
        return 1

    return None



# ----------------------------
# BUILD LINE INPUT (price preservation)
# ----------------------------
def build_line_input(line: Dict[str, Any]) -> Dict[str, Any]:
    """
    Key detail:
    - For variant lines, originalUnitPriceWithCurrency is ignored when variantId is set.
      To preserve the original draft pricing, we use priceOverride:
        - prefer existing priceOverride
        - else use originalUnitPriceWithCurrency as priceOverride
    - For custom lines (no variant), we can use originalUnitPriceWithCurrency.
    """
    out: Dict[str, Any] = {"quantity": int(line.get("quantity") or 0)}

    if line.get("variant"):
        out["variantId"] = line["variant"]["id"]

        # Preserve pricing
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
    """Best-effort line count from the draftOrderUpdate payload."""
    try:
        li = (draft_node.get("lineItems") or {})
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
    """Run draftOrderUpdate and return (userErrors, draftOrder node)."""
    if DRY_RUN:
        print(f"    DRY RUN — would update {label}: {draft_id}")
        return [], {}
    res = gql(MUTATION_UPDATE, {"id": draft_id, "input": input_data})["draftOrderUpdate"]
    errs = res.get("userErrors") or []
    d = res.get("draftOrder") or {}
    if not errs:
        print(f"    Updated {label}: {d.get('name')} | poNumber={d.get('poNumber')}")
    return errs, d


def draft_update_strict(draft_id: str, input_data: Dict[str, Any], label: str) -> None:
    """Run draftOrderUpdate and raise on userErrors."""
    errs, d_last = draft_update_return(draft_id, input_data, label)
    if errs:
        raise RuntimeError(f"draftOrderUpdate userErrors ({label}): {errs}")


def draft_delete(draft_id: str, label: str) -> None:
    """Delete a draft order (best-effort rollback)."""
    if DRY_RUN:
        print(f"    DRY RUN — would delete {label}: {draft_id}")
        return

    try:
        res = gql(MUTATION_DELETE, {"id": draft_id})["draftOrderDelete"]
        errs = res.get("userErrors") or []
        if errs:
            print(f"    WARNING: draftOrderDelete userErrors ({label}): {errs}")
        else:
            print(f"    Deleted {label}: {draft_id}")
    except Exception as e:
        print(f"    WARNING: failed to delete {label} {draft_id}: {e}")


# ----------------------------
# CHILD LOOKUP (bucket-level idempotency)
# ----------------------------
def find_existing_child(original_draft_id: str, bucket: int) -> Optional[Dict[str, str]]:
    """
    Returns {'id':..., 'name':...} for an existing child draft for this original + bucket, if found.

    We look for drafts tagged:
      - split-backorder-child
      - Backorder #{bucket}

    and whose ORIGINAL_DRAFT_ID metafield equals the original_draft_id.
    """
    q = f'tag:"Backorder #{bucket}" tag:split-backorder-child status:open'
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
            node = (e.get("node") or {})
            link = (node.get("link") or {})
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
    """
    Best-effort customer label that does NOT require read_customers scope.
    """
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
    """
    Upsert the row into csv_path using key_field as the unique key.
    Creates the file with headers if missing.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows: List[Dict[str, str]] = []
    headers: List[str] = []

    if csv_path.exists():
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            existing_rows = list(reader)

    # Ensure headers include all row keys (and keep a stable order)
    def ensure_header(h: str) -> None:
        if h not in headers:
            headers.append(h)

    for k in row.keys():
        ensure_header(k)

    key_val = row.get(key_field, "")
    replaced = False
    for i, r in enumerate(existing_rows):
        if (r.get(key_field) or "") == key_val and key_val:
            # Update existing row
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

# Net terms inference from draft note (fallback when paymentTerms is blank)
_VALID_NET_DAYS = {30, 60, 90, 120}
_NET_PATTERNS = [
    re.compile(r"\bnet\s*[-:]*\s*(30|60|90|120)\b", re.IGNORECASE),
    re.compile(r"\bnet(30|60|90|120)\b", re.IGNORECASE),
    re.compile(r"\bterms?\s*[:\-]?\s*net\s*(30|60|90|120)\b", re.IGNORECASE),
    re.compile(r"\bn\s*(30|60|90|120)\b", re.IGNORECASE),
]

def infer_net_days_from_note(note_text: str) -> int:
    """Return net days (30/60/90/120) if exactly one clear term is found; else 0."""
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
    """Map days to env var PAYMENT_TERMS_TEMPLATE_ID_NET{days}."""
    if not days:
        return ""
    return (os.getenv(f"PAYMENT_TERMS_TEMPLATE_ID_NET{days}") or "").strip()

# ----------------------------
def payment_terms_template_id_from_draft(draft: Dict[str, Any]) -> str:
    """Best-effort: map draft.paymentTerms.dueInDays to env PAYMENT_TERMS_TEMPLATE_ID_NET{days}."""
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
    """Return DraftOrderInput.paymentTerms payload (template-only).

    Note: Some Net terms templates require an issue date; we handle that by retrying with issuedAt
    if Shopify returns "issue date is required".
    """
    if not template_id:
        return None
    return {
        "paymentTermsTemplateId": template_id,
    }

def build_payment_terms_input_with_issue_date(template_id: str) -> Optional[Dict[str, Any]]:
    """Return DraftOrderInput.paymentTerms payload including an issue date schedule (issuedAt)."""
    if not template_id:
        return None
    issued_at = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    return {
        "paymentTermsTemplateId": template_id,
        "paymentSchedules": {"issuedAt": issued_at},
    }

# ----------------------------
def apply_update_with_retries(
    *,
    draft_id: str,
    input_data: Dict[str, Any],
    label: str,
    terms_template_id: str = "",
) -> Dict[str, Any]:
    """Update a draft with a couple of safe retries for known Shopify constraints."""
    # First attempt
    errs, d_last = draft_update_return(draft_id, input_data, label)
    if not errs:
        return d_last

    msg_join = " | ".join((e.get("message", "") or "") for e in errs).lower()

    # Retry 1: Net terms templates sometimes require an issuedAt schedule.
    if "issue date is required" in msg_join and terms_template_id:
        pti2 = build_payment_terms_input_with_issue_date(terms_template_id)
        if pti2:
            input_data = dict(input_data)  # shallow copy
            input_data["paymentTerms"] = pti2
        errs2, d2 = draft_update_return(draft_id, input_data, label=f"{label} (retry with issuedAt)")
        if not errs2:
            return d2
        errs = errs2
        msg_join = " | ".join((e.get("message", "") or "") for e in errs).lower()

    # Retry 2: When no remaining items require shipping, Shopify rejects shippingLine.
    if "cannot add shipping when no line items require shipping" in msg_join:
        input_data = dict(input_data)
        input_data["shippingLine"] = None
        errs3, d3 = draft_update_return(draft_id, input_data, label=f"{label} (retry without shippingLine)")
        if not errs3:
            return d3
        errs = errs3

    raise RuntimeError(f"draftOrderUpdate userErrors ({label}): {errs}")

def process_draft(draft_id: str) -> str:
    draft = gql(QUERY_DRAFT_DETAIL, {"id": draft_id, "locationId": LOCATION_ID, "poNamespace": PO_METAFIELD_NAMESPACE, "poKey": PO_METAFIELD_KEY})["draftOrder"]
    if not draft:
        print(f"Draft not found: {draft_id}")
        return

    # Optional: process only exact names (normalized)
    if DRAFT_ORDER_NAMES:
        targets = {normalize_draft_name(n) for n in DRAFT_ORDER_NAMES}
        if normalize_draft_name(draft.get("name", "")) not in targets:
            print(f"{draft.get('name')}: SKIP (not in DRAFT_ORDER_NAMES)")
            return

    # Idempotency guard
    existing_tags = set(draft.get("tags") or [])

    # Never process child drafts as originals
    if "split-backorder-child" in existing_tags:
        print(f"{draft.get('name')}: SKIP (is a split child; tag 'split-backorder-child' present).")
        return

    if IDEMPOTENCY_DONE_TAG in existing_tags:
        print(f"{draft['name']}: SKIP (already processed; tag '{IDEMPOTENCY_DONE_TAG}' present).")
        return "skipped"

    lines = (draft.get("lineItems") or {}).get("nodes") or []

    # Snapshot full original lineItems input so we can restore if something goes wrong later.
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
        print(f"{draft['name']}: no backorders needed.")
        return "skipped"

    # Choose primary lines to keep on original so it remains valid.
    primary_bucket_for_original: Optional[int] = None
    if not keep:
        # No ship-now lines. Keep smallest non-empty bucket on original.
        non_empty = [b for b, ls in buckets.items() if ls]
        primary_bucket_for_original = min(non_empty) if non_empty else None
        if primary_bucket_for_original is not None:
            keep = buckets[primary_bucket_for_original]
            buckets[primary_bucket_for_original] = []

    if not keep:
        # Should not happen, but protect.
        print(f"{draft['name']}: ERROR — after selection, original would have 0 line items. Skipping.")
        return "skipped"

    existing_po_meta = (draft.get("po_meta") or {}).get("value")
    base_po = (existing_po_meta or draft.get("poNumber") or "").strip()
    original_po = base_po
    # Payment terms: capture template id from original (preferred), else fallback env.
    note_text = (draft.get("note2") or "")
    net_days = infer_net_days_from_note(note_text)
    note_terms_template_id = template_id_for_net_days(net_days)
    # Payment terms: capture template id from original (preferred), else infer from note, else fallback env.
    original_terms_template_id = payment_terms_template_id_from_draft(draft) or note_terms_template_id or PAYMENT_TERMS_TEMPLATE_ID_FALLBACK
    if original_terms_template_id:
        print(f"  Payment terms template id: {original_terms_template_id}")
    else:
        print("  Payment terms template id: (none)")


    original_name = draft.get("name")
    original_tags = list(draft.get("tags") or [])
    original_custom_attributes = (draft.get("customAttributes") or [])

    print(f"\nProcessing {original_name} (DRY_RUN={DRY_RUN})")
    newly_created_child_ids: List[Tuple[int,str]] = []  # (bucket, draft_id)
    print(f"  Original PO: {original_po!r}")
    if primary_bucket_for_original is None:
        print(f"  Original keeps ship-now lines: {len(keep)}")
    else:
        print(f"  Original assigned to bucket #{primary_bucket_for_original}: {len(keep)}")

    # Build additions for linkage
    ca_add_orig, mf_add_orig = build_linking_fields(base_po=base_po, original_draft_id=draft_id, is_child=False)
    bo_draft_ids: Dict[int, str] = {}

    # For children we build per-bucket below    # 1) Create & update backorder drafts

    for bucket, bucket_lines in buckets.items():
        if not bucket_lines:
            continue

        # Bucket-level idempotency: reuse existing child draft if present
        existing_child = None if DRY_RUN else find_existing_child(draft_id, bucket)
        created_new = False

        if existing_child:
            dup_id = existing_child["id"]
            dup_name = existing_child.get("name") or ""
            print(f"  Child bucket #{bucket}: {len(bucket_lines)} line(s) | PO={build_po_number(original_po, bucket)}")
            print(f"    Reusing existing child: {dup_name} ({dup_id})")
        else:
            dup = draft_duplicate(draft_id)
            dup_id = dup["id"]
            created_new = (not DRY_RUN)
            if created_new:
                newly_created_child_ids.append((bucket, dup_id))
            dup_name = dup.get("name") or ""
            print(f"  Child bucket #{bucket}: {len(bucket_lines)} line(s) | PO={build_po_number(original_po, bucket)}")
            if DRY_RUN:
                print("    DRY RUN — would duplicate original and update duplicate lineItems/poNumber/tags/metafield.")
            else:
                print(f"    Duplicated: {dup_name} ({dup_id})")

        bo_draft_ids[bucket] = (f"DRY_RUN_BO{bucket}" if DRY_RUN else dup_id)

        # Build tags
        new_tags = list(original_tags)
        bucket_tag = f"Backorder #{bucket}"
        if bucket_tag not in new_tags:
            new_tags.append(bucket_tag)
        child_tag = "split-backorder-child"
        if child_tag not in new_tags:
            new_tags.append(child_tag)

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
            "metafields": mf_add_child,
        }

        # Ensure payment terms on children if a template id was determined.
        if SET_PAYMENT_TERMS_ON_CHILDREN and original_terms_template_id:
            pti = build_payment_terms_input(original_terms_template_id)
            if pti:
                update_input["paymentTerms"] = pti        # Update child; on failure, roll back newly created children and abort BEFORE touching original.
        try:
            apply_update_with_retries(
                draft_id=dup_id,
                input_data=update_input,
                label=f"child bucket #{bucket}",
                terms_template_id=original_terms_template_id,
            )
        except Exception as e:
            print(f"    ERROR updating child bucket #{bucket}: {e}")
            for b, did in reversed(newly_created_child_ids):
                draft_delete(did, label=f"rollback child bucket #{b}")
            raise

    # 2) Update original draft with keep lines + idempotency
    updated_tags = list(original_tags)
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
        "metafields": mf_add_orig,
        # Keep original poNumber unchanged.
    }

    print(f"  Updating original: keep {len(keep)} line(s) + tag '{IDEMPOTENCY_DONE_TAG}'")

    # In DRY_RUN we only print what would happen; do not verify or roll back.
    if DRY_RUN:
        draft_update_return(draft_id, original_update, label="original")
        return "success"

    try:
        updated_node = apply_update_with_retries(draft_id=draft_id, input_data=original_update, label="original", terms_template_id=original_terms_template_id)

        # Verify original update using mutation response (fast, no extra query)
        v_tags = set(updated_node.get("tags") or [])
        v_total = get_lineitems_total_count(updated_node)
        if IDEMPOTENCY_DONE_TAG not in v_tags or (v_total is not None and v_total != len(keep)):
            raise RuntimeError(
                f"post-update verification failed: tag_present={IDEMPOTENCY_DONE_TAG in v_tags} "
                f"line_count={v_total} expected={len(keep)}"
            )
        print(f"    Verified original updated: {updated_node.get('name')} | lines={v_total}")
        return "success"
    except Exception as e:
        print(f"    ERROR updating/verifying original: {e}")
        # Roll back any NEW child drafts created in this run (best-effort)
        for b, did in reversed(newly_created_child_ids):
            draft_delete(did, label=f"rollback child bucket #{b}")

        # Restore original lineItems and tags to pre-run state (best-effort)
        restore_tags = list(original_tags)
        restore_tags = [t for t in restore_tags if t != IDEMPOTENCY_DONE_TAG]
        restore_input: Dict[str, Any] = {
            "lineItems": original_full_line_items_input,
            "tags": restore_tags,
            "customAttributes": original_custom_attributes,
        }
        errs_restore, _d_restore = draft_update_return(draft_id, restore_input, label="restore original")
        if errs_restore:
            print(f"    WARNING: restore original userErrors: {errs_restore}")
        raise

    # ----------------------------
    # Write / update CSV log
    # ----------------------------
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

    upsert_split_log_row(csv_path=Path(__file__).resolve().parent / LOG_CSV_PATH, row=row)
    return "success"



# ----------------------------
# MAIN
# ----------------------------
def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i+size] for i in range(0, len(items), size)]


# ----------------------------
# MAIN
# ----------------------------
def main() -> None:
    targets = {normalize_draft_name(n) for n in DRAFT_ORDER_NAMES} if DRAFT_ORDER_NAMES else set()

    collected: List[Dict[str, Any]] = []
    scanned = 0

    if DRAFT_ORDER_NAMES:
        # OPTION B (robust): Query Shopify in chunks by draft name, instead of scanning thousands of open drafts.
        # This avoids missing older drafts when the open-draft list is huge.
        # We still add status:open to avoid pulling completed drafts.
        CHUNK_SIZE = 12  # keep query strings comfortably under Shopify limits
        for chunk in chunk_list(DRAFT_ORDER_NAMES, CHUNK_SIZE):
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
        # Default: scan a bounded number of open drafts
        query = "status:open"
        page_size = min(250, MAX_DRAFTS)
        after = None
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
        print("No drafts found.")
        return

    # De-dupe by ID (chunks can overlap)
    dedup: Dict[str, Dict[str, Any]] = {}
    for d in collected:
        did = d.get("id")
        if did and did not in dedup:
            dedup[did] = d
    drafts = list(dedup.values())

    if DRAFT_ORDER_NAMES:
        drafts = [d for d in drafts if normalize_draft_name(d.get("name", "")) in targets]

    print(f"Found {len(drafts)} draft(s) AFTER client-side filter. DRY_RUN={DRY_RUN} (scanned {scanned} draft rows from API)")
    if DRAFT_ORDER_NAMES and not drafts:
        sample = [d.get("name","") for d in list(dedup.values())[:25]]
        print("Sample of returned drafts:", ", ".join(sample))

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
            print(f"{draft_name}: FAILED — {e}")
            continue

    print("\nRun summary")
    print(f"  SUCCESS: {len(successes)}")
    if successes:
        print("    " + ", ".join(successes))
    print(f"  SKIPPED: {len(skipped)}")
    if skipped:
        print("    " + ", ".join(skipped))
    print(f"  FAILED: {len(failed)}")
    if failed:
        for draft_name, err in failed:
            print(f"    {draft_name}: {err}")


if __name__ == "__main__":
    main()
