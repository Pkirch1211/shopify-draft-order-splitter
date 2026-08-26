import os
import re
import time
import datetime
import logging
from datetime import timezone
from decimal import Decimal, InvalidOperation
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


def env_decimal(*names: str, default: str) -> Decimal:
    value = env_first(*names)
    try:
        return Decimal(str(value if value is not None else default))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


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


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip().casefold()
    text = re.sub(r"\s+", " ", text)
    return text


def contains_any_substring(haystack: str, needles: Set[str]) -> List[str]:
    if not haystack or not needles:
        return []
    return [n for n in sorted(needles) if n and n in haystack]


def to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        if value is None or value == "":
            return Decimal(default)
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


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

# ----------------------------
# NEW LOGIC CONFIG
# ----------------------------
# The GitHub-editable threshold for the "ships now" (keep) side of a split.
# This is the PRIORITY gate: if the keep side doesn't independently clear
# this value, the order is never split, no matter what the backorder side
# is worth.
MIN_SPLIT_VALUE = env_decimal("MIN_SPLIT_VALUE", default="150")

# The GitHub-editable threshold for the backorder ("stays behind") side.
# This is intentionally LOWER than MIN_SPLIT_VALUE. It only governs whether
# a split is attempted at all once the keep side has already cleared
# MIN_SPLIT_VALUE. It does NOT relax the keep-side requirement in any way.
MIN_BACKORDER_HOLD_VALUE = env_decimal("MIN_BACKORDER_HOLD_VALUE", default="75")

# Once a split is attempted and verified, the backorder child is tagged
# with one of these two "value band" tags based on its ACTUAL (post
# verification) value, in addition to BACKORDER_CHILD_TAG:
#   MIN_BACKORDER_HOLD_VALUE <= bo_value < MIN_SPLIT_VALUE  -> SPLIT_REMAINDER_TAG
#   bo_value >= MIN_SPLIT_VALUE                              -> SPLIT_150_TAG
SPLIT_REMAINDER_TAG = env_first("SPLIT_REMAINDER_TAG", default="split-remainder") or "split-remainder"
SPLIT_150_TAG = env_first("SPLIT_150_TAG", default="split-150") or "split-150"

# Any product tag starting with this prefix is treated as a launch/embargo
# signal: the item cannot ship even if it is physically in stock.
LAUNCH_TAG_PREFIX = (env_first("LAUNCH_TAG_PREFIX", default="launch-") or "launch-").casefold()

# Allow-list gate. Only drafts carrying this tag are ever touched by this
# script. It is stamped onto new drafts at import time by shopify-orders-all-open.py.
# Legacy drafts (created before this rollout) will never carry it and are
# therefore invisible to this script, full stop.
ORDER_FLOW_TAG = env_first("ORDER_FLOW_TAG", default="order-flow-version2") or "order-flow-version2"

# Concurrency lock, mirrors the pattern used in the legacy script but with its
# own tag so the two pipelines never contend with each other.
PROCESSING_TAG = env_first("V2_PROCESSING_TAG", default="v2-processing") or "v2-processing"

# Stamped on a draft (parent or otherwise-final) once this script has fully
# evaluated it, regardless of outcome. Prevents re-evaluating the same order
# forever. This is distinct from ORDER_FLOW_TAG, which never gets removed.
EVAL_DONE_TAG = env_first("V2_EVAL_DONE_TAG", default="eval-done") or "eval-done"

# Applied to the single backorder child created on a successful split.
BACKORDER_CHILD_TAG = env_first("BACKORDER_CHILD_TAG", default="split1") or "split1"

# The three outcome tags from the 2x2 matrix.
INSTOCK_MINVALUE_TAG = env_first("INSTOCK_MINVALUE_TAG", default="instock-minvalue") or "instock-minvalue"
BO_MINVALUE_TAG = env_first("BO_MINVALUE_TAG", default="BO-minvalue") or "BO-minvalue"
ORDER_MINVALUE_TAG = env_first("ORDER_MINVALUE_TAG", default="order-minvalue") or "order-minvalue"
ALL_MINVALUE_TAGS = {INSTOCK_MINVALUE_TAG, BO_MINVALUE_TAG, ORDER_MINVALUE_TAG}

NEEDS_REVIEW_TAG = env_first("NEEDS_REVIEW_TAG", default="needs-review") or "needs-review"

PO_SUFFIX_FORMAT = env_first("PO_SUFFIX_FORMAT", default=" - BO1") or " - BO1"

# Tags that can cause other automations to convert a draft into an order.
# Removed before duplicating so an orphan duplicate cannot inherit a
# converter trigger tag if Shopify creates the duplicate but the response
# is lost before this script can update/tag the child.
CONVERSION_TRIGGER_TAGS = parse_csv_set(
    env_first("CONVERSION_TRIGGER_TAGS", default="instock-ready"),
    casefold=False,
)

# Exact-match / substring customer exclusions, same mechanism as the legacy script.
EXCLUDED_CUSTOMERS = parse_csv_set(env_first("EXCLUDED_CUSTOMERS", default=""), casefold=True)
DEFAULT_EXCLUDED_SUBSTRINGS = {
    "faire",
    "faire marketplace",
    "customer samples",
    "tjx canada",
    "tjx companies",
    "replacements and customer care",
    "replacements customer care customer care",
    "noreen batdorf",
    "norman's hallmark",
}
EXCLUDED_CUSTOMER_SUBSTRINGS = parse_csv_set(
    env_first("EXCLUDED_CUSTOMER_SUBSTRINGS", default=""),
    casefold=True,
) or set()
EXCLUDED_CUSTOMER_SUBSTRINGS = set(EXCLUDED_CUSTOMER_SUBSTRINGS).union(DEFAULT_EXCLUDED_SUBSTRINGS)

# Linking fields (parent <-> child), same shape as the legacy script.
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

ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE = env_first("ORIGINAL_DRAFT_ID_METAFIELD_NAMESPACE", default="custom") or "custom"
ORIGINAL_DRAFT_ID_METAFIELD_KEY = env_first("ORIGINAL_DRAFT_ID_METAFIELD_KEY", default="original_draft_id") or "original_draft_id"
ORIGINAL_DRAFT_ID_METAFIELD_TYPE = env_first("ORIGINAL_DRAFT_ID_METAFIELD_TYPE", default="single_line_text_field") or "single_line_text_field"

PAYMENT_TERMS_TEMPLATE_ID_FALLBACK = env_first("PAYMENT_TERMS_TEMPLATE_ID", default="") or ""

print("SHOPIFY_SHOP =", SHOP)
print("API_VERSION  =", API_VERSION)
print("DRAFT_ORDER_NAMES =", DRAFT_ORDER_NAMES)
print("DRY_RUN =", DRY_RUN)
print("MIN_SPLIT_VALUE (keep-side gate) =", MIN_SPLIT_VALUE)
print("MIN_BACKORDER_HOLD_VALUE (backorder hold gate) =", MIN_BACKORDER_HOLD_VALUE)
print("SPLIT_REMAINDER_TAG =", SPLIT_REMAINDER_TAG)
print("SPLIT_150_TAG =", SPLIT_150_TAG)
print("LAUNCH_TAG_PREFIX =", LAUNCH_TAG_PREFIX)
print("ORDER_FLOW_TAG (allow-list) =", ORDER_FLOW_TAG)
print("PROCESSING_TAG =", PROCESSING_TAG)
print("EVAL_DONE_TAG =", EVAL_DONE_TAG)
print("BACKORDER_CHILD_TAG =", BACKORDER_CHILD_TAG)
print("MINVALUE TAGS =", INSTOCK_MINVALUE_TAG, "|", BO_MINVALUE_TAG, "|", ORDER_MINVALUE_TAG)
print("EXCLUDED_CUSTOMERS =", sorted(EXCLUDED_CUSTOMERS))
print("EXCLUDED_CUSTOMER_SUBSTRINGS =", sorted(EXCLUDED_CUSTOMER_SUBSTRINGS))

if not SHOP or not TOKEN:
    raise SystemExit(
        "Missing shop/token env vars. Accepted names:\n"
        "  SHOPIFY_SHOP or SHOPIFY_STORE\n"
        "  SHOPIFY_ADMIN_ACCESS_TOKEN or SHOPIFY_TOKEN"
    )
if not LOCATION_ID:
    raise SystemExit("Missing location env var. Accepted names:\n  SHOPIFY_LOCATION_ID or LOCATION_ID")

GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("shopify-adjust-orders-v2")


def normalize_draft_name(name: str) -> str:
    if not name:
        return ""
    s = str(name).strip()
    s = s.replace("Draft", "").strip()
    if s.startswith("#"):
        s = s[1:]
    return s.strip().upper()


def normalize_customer_name(name: str) -> str:
    return normalize_text(name)


def candidate_customer_labels(draft: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for addr_key in ("shippingAddress", "billingAddress"):
        addr = draft.get(addr_key) or {}
        for field in ("company", "name"):
            v = addr.get(field)
            if v:
                out.append(str(v))
    email = draft.get("email")
    if email:
        out.append(str(email))
    return out


def build_exclusion_haystack(draft: Dict[str, Any]) -> Tuple[List[str], str]:
    vals = candidate_customer_labels(draft)
    blob = normalize_text(" | ".join(vals))
    return vals, blob


def is_excluded_draft(draft: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    customer_candidates = candidate_customer_labels(draft)
    customer_candidate_norms = {normalize_customer_name(x) for x in customer_candidates}
    exact_matches = sorted(customer_candidate_norms.intersection(EXCLUDED_CUSTOMERS))

    haystack_vals, haystack_blob = build_exclusion_haystack(draft)
    substring_matches = contains_any_substring(haystack_blob, EXCLUDED_CUSTOMER_SUBSTRINGS)

    matched_reasons: List[str] = []
    if exact_matches:
        matched_reasons.append(f"exact customer match: {', '.join(exact_matches)}")
    if substring_matches:
        matched_reasons.append(f"substring match: {', '.join(substring_matches)}")

    details = {
        "customer_candidates": customer_candidates,
        "exact_matches": exact_matches,
        "substring_matches": substring_matches,
        "haystack_values": haystack_vals,
        "matched_reasons": matched_reasons,
    }
    return bool(exact_matches or substring_matches), details


def build_draft_name_query(names: List[str]) -> str:
    vals: List[str] = []
    seen = set()
    for n in names:
        raw = str(n).strip()
        if not raw:
            continue
        base = raw.lstrip("#").strip()
        for c in (raw, base, f"#{base}"):
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


def build_po_number(original_po: Optional[str]) -> str:
    base = (original_po or "").strip()
    if not base:
        return "BACKORDER-1"
    if base.endswith(PO_SUFFIX_FORMAT):
        return base
    return base + PO_SUFFIX_FORMAT


# ----------------------------
# GRAPHQL
# ----------------------------
def gql(query: str, variables: Optional[Dict[str, Any]] = None, *, attempts: int = 5) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": TOKEN}
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
                raise RuntimeError(f"HTTP {resp.status_code} calling Shopify GraphQL.\nResponse:\n{resp.text}")
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
    paymentTerms { dueInDays paymentTermsName paymentTermsType }
    customAttributes { key value }
    po_meta: metafield(namespace: $poNamespace, key: $poKey) { value }
    ship_date_meta: metafield(namespace: $shipDateNamespace, key: $shipDateKey) { value }
    metafields(first:250) { nodes { namespace key type value } }
    lineItems(first:250) {
      nodes {
        quantity
        title
        appliedDiscount {
          description title value valueType
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
    draftOrder { id name tags poNumber lineItems(first: 250) { edges { node { id } } } }
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


def fetch_draft_detail(draft_id: str) -> Dict[str, Any]:
    data = gql(
        QUERY_DRAFT_DETAIL,
        {
            "id": draft_id,
            "locationId": LOCATION_ID,
            "poNamespace": PO_METAFIELD_NAMESPACE,
            "poKey": PO_METAFIELD_KEY,
            "shipDateNamespace": SHIP_DATE_METAFIELD_NAMESPACE,
            "shipDateKey": SHIP_DATE_METAFIELD_KEY,
        },
    )
    return data.get("draftOrder") or {}


# ----------------------------
# MONEY / INPUT HELPERS
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
        k, v = item.get("key"), item.get("value")
        if k:
            merged[str(k)] = "" if v is None else str(v)
    for item in additions or []:
        k, v = item.get("key"), item.get("value")
        if k:
            merged[str(k)] = "" if v is None else str(v)
    return [{"key": k, "value": v} for k, v in merged.items()]


def merge_metafields(existing: List[Dict[str, Any]], additions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in list(existing or []) + list(additions or []):
        ns = (item.get("namespace") or "").strip()
        key = (item.get("key") or "").strip()
        if not ns or not key:
            continue
        value = item.get("value")
        if value is None:
            continue
        merged[(ns, key)] = {
            "namespace": ns,
            "key": key,
            "type": (item.get("type") or "").strip(),
            "value": str(value),
        }
    return list(merged.values())


def build_linking_fields(*, base_po: str, original_draft_id: str, is_child: bool) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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
    if is_child and base_po:
        mf_add.append(
            {
                "namespace": PO_METAFIELD_NAMESPACE,
                "key": PO_METAFIELD_KEY,
                "type": PO_METAFIELD_TYPE,
                "value": build_po_number(base_po),
            }
        )
    return ca_add, mf_add


def parse_ship_date_value(raw: Optional[str]) -> Optional[datetime.date]:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        if "T" in text:
            return datetime.datetime.fromisoformat(text.replace("Z", "+00:00")).date()
        return datetime.date.fromisoformat(text)
    except Exception:
        return None


def ship_date_is_eligible(raw_ship_date: Optional[str]) -> Tuple[bool, Optional[datetime.date], datetime.date]:
    # Unchanged from the legacy script: eligible only if the ship date is
    # empty, or already today/in the past. This is intentionally stricter
    # than the 7-day window used elsewhere in the pipeline and is preserved
    # as-is per the "don't touch what isn't part of this change" rule.
    ship_date = parse_ship_date_value(raw_ship_date)
    today = datetime.datetime.now().date()
    tomorrow = today + datetime.timedelta(days=1)
    if raw_ship_date is None or str(raw_ship_date).strip() == "":
        return True, None, tomorrow
    if ship_date is None:
        return False, None, tomorrow
    return ship_date < tomorrow, ship_date, tomorrow


# ----------------------------
# RULE ENGINE — truth table + dual gate (asymmetric thresholds)
# ----------------------------
def get_available_qty(line: Dict[str, Any]) -> Optional[int]:
    try:
        variant = line.get("variant") or {}
        inv_item = variant.get("inventoryItem") or {}
        if inv_item.get("tracked") is False:
            return None
        level = inv_item.get("inventoryLevel")
        if not level:
            return 0
        for q in (level.get("quantities") or []):
            if q.get("name") == "available":
                return int(q.get("quantity") or 0)
        return 0
    except Exception:
        return None


def has_launch_tag(line: Dict[str, Any]) -> bool:
    variant = line.get("variant") or {}
    tags = (variant.get("product") or {}).get("tags") or []
    return any(str(t).strip().casefold().startswith(LAUNCH_TAG_PREFIX) for t in tags)


def is_fully_in_stock(line: Dict[str, Any]) -> bool:
    variant = line.get("variant")
    if not variant:
        # Custom (non-variant) line items have no inventory concept — treat as in stock.
        return True
    qty = int(line.get("quantity") or 0)
    available = get_available_qty(line)
    if available is None:
        # Untracked inventory item — never held back for stock reasons.
        return True
    return available >= qty


def get_line_unit_price(line: Dict[str, Any]) -> Decimal:
    override = line.get("priceOverride") or {}
    if override.get("amount") is not None:
        return to_decimal(override.get("amount"))
    orig = line.get("originalUnitPriceWithCurrency") or {}
    if orig.get("amount") is not None:
        return to_decimal(orig.get("amount"))
    return Decimal("0")


def line_value(line: Dict[str, Any]) -> Decimal:
    qty = int(line.get("quantity") or 0)
    return get_line_unit_price(line) * Decimal(qty)


def classify_lines(lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    The only combination that ships now is: no launch tag AND fully in stock.
    Everything else (plain out-of-stock, launch-tagged-but-in-stock,
    launch-tagged-and-out-of-stock) goes into the single backorder group.
    """
    keep: List[Dict[str, Any]] = []
    backorder: List[Dict[str, Any]] = []
    for line in lines:
        if not has_launch_tag(line) and is_fully_in_stock(line):
            keep.append(line)
        else:
            backorder.append(line)
    return keep, backorder


def sum_value(lines: List[Dict[str, Any]]) -> Decimal:
    total = Decimal("0")
    for line in lines:
        total += line_value(line)
    return total


def pick_minvalue_tag(keep_ok: bool, bo_ok: bool) -> str:
    """
    Picks the reason tag for a full unwind (no split attempted / split
    reversed). `bo_ok` here is ALWAYS evaluated against MIN_SPLIT_VALUE
    ($150), never against MIN_BACKORDER_HOLD_VALUE ($75) — this function is
    purely diagnostic labeling and its thresholds did not change. The
    decision of *whether* to unwind is handled separately by the caller.
    """
    if not keep_ok and bo_ok:
        return INSTOCK_MINVALUE_TAG
    if keep_ok and not bo_ok:
        return BO_MINVALUE_TAG
    return ORDER_MINVALUE_TAG


def pick_split_band_tag(bo_value: Decimal) -> str:
    """
    Picks the value-band tag for a SUCCESSFUL split, based on the backorder
    child's value. Only ever called once bo_value has already cleared
    MIN_BACKORDER_HOLD_VALUE, so this is strictly choosing between the two
    "we did split" bands, not deciding whether to split.
    """
    if bo_value >= MIN_SPLIT_VALUE:
        return SPLIT_150_TAG
    return SPLIT_REMAINDER_TAG


# ----------------------------
# LINE INPUT BUILDER
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


# ----------------------------
# MUTATION WRAPPERS
# ----------------------------
def draft_duplicate(original_id: str) -> Dict[str, Any]:
    if DRY_RUN:
        return {"id": "DRY_RUN_DUPLICATE", "name": "DRY_RUN_DUPLICATE"}
    # Non-idempotent: never retry this call automatically.
    res = gql(MUTATION_DUPLICATE, {"id": original_id}, attempts=1)["draftOrderDuplicate"]
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
# TAG / LOCK HELPERS
# ----------------------------
def with_tag(tags: List[str], tag: str) -> List[str]:
    out = list(tags or [])
    if tag not in out:
        out.append(tag)
    return out


def without_tag(tags: List[str], tag: str) -> List[str]:
    return [t for t in (tags or []) if t != tag]


def without_tags(tags: List[str], tags_to_remove: Set[str]) -> List[str]:
    remove = set(tags_to_remove or set())
    return [t for t in (tags or []) if t not in remove]


def claim_processing_lock(draft: Dict[str, Any]) -> bool:
    tags = list(draft.get("tags") or [])
    if PROCESSING_TAG in tags or EVAL_DONE_TAG in tags or NEEDS_REVIEW_TAG in tags:
        return False
    if BACKORDER_CHILD_TAG in tags:
        return False
    if DRY_RUN:
        logger.info("DRY RUN — would add processing tag to %s", draft.get("name"))
        return True
    new_tags = without_tags(with_tag(tags, PROCESSING_TAG), CONVERSION_TRIGGER_TAGS)
    errs, updated = draft_update_return(draft["id"], {"tags": new_tags}, label="claim processing lock")
    if errs:
        raise RuntimeError(f"Failed to claim processing lock: {errs}")
    return PROCESSING_TAG in set(updated.get("tags") or [])


def release_processing_lock(draft_id: str, tags: List[str]) -> None:
    if DRY_RUN:
        logger.info("DRY RUN — would remove processing tag from %s", draft_id)
        return
    cleaned = without_tag(tags, PROCESSING_TAG)
    errs, _ = draft_update_return(draft_id, {"tags": cleaned}, label="release processing lock")
    if errs:
        logger.warning("Failed to release processing lock for %s: %s", draft_id, errs)


# ----------------------------
# DRAFT PROCESSOR
# ----------------------------
def process_draft(draft_id: str) -> str:
    draft = fetch_draft_detail(draft_id)
    name = draft.get("name", draft_id)
    existing_tags = list(draft.get("tags") or [])

    # --- allow-list gate: only new-logic orders ever get this far ---
    if ORDER_FLOW_TAG not in existing_tags:
        logger.info("%s: SKIP (missing allow-list tag '%s').", name, ORDER_FLOW_TAG)
        return "skipped"

    if EVAL_DONE_TAG in existing_tags:
        logger.info("%s: SKIP (already evaluated; tag '%s' present).", name, EVAL_DONE_TAG)
        return "skipped"
    if BACKORDER_CHILD_TAG in existing_tags:
        logger.info("%s: SKIP (is a backorder child; tag '%s' present).", name, BACKORDER_CHILD_TAG)
        return "skipped"
    if NEEDS_REVIEW_TAG in existing_tags:
        logger.info("%s: SKIP (tag '%s' present).", name, NEEDS_REVIEW_TAG)
        return "skipped"
    if PROCESSING_TAG in existing_tags:
        logger.info("%s: SKIP (tag '%s' present — concurrent run?).", name, PROCESSING_TAG)
        return "skipped"

    excluded, exclusion_details = is_excluded_draft(draft)
    if excluded:
        logger.info("%s: SKIP (excluded customer: %s).", name, " ; ".join(exclusion_details["matched_reasons"]))
        return "skipped"

    raw_ship_date = ((draft.get("ship_date_meta") or {}).get("value") or "").strip()
    ship_ok, parsed_ship_date, _ = ship_date_is_eligible(raw_ship_date)
    if not ship_ok:
        logger.info("%s: SKIP (ship date %r not yet eligible).", name, raw_ship_date)
        return "skipped"

    if not claim_processing_lock(draft):
        logger.info("%s: SKIP (could not claim processing lock).", name)
        return "skipped"

    processing_released = False
    try:
        live = fetch_draft_detail(draft_id)  # re-fetch fresh after claiming the lock
        lines = (live.get("lineItems") or {}).get("nodes") or []
        keep_lines, backorder_lines = classify_lines(lines)

        if not backorder_lines:
            logger.info("%s: fully ships now, no backorder items. Tagging '%s'.", name, EVAL_DONE_TAG)
            final_tags = with_tag(without_tag(list(live.get("tags") or []), PROCESSING_TAG), EVAL_DONE_TAG)
            draft_update_return(draft_id, {"tags": final_tags}, label="tag eval-done (no split needed)")
            processing_released = True
            return "processed"

        keep_value = sum_value(keep_lines)
        bo_value = sum_value(backorder_lines)

        # PRIORITY GATE: the ships-now side always uses the full
        # MIN_SPLIT_VALUE ($150). If it fails, nothing else matters — the
        # order is never split, regardless of the backorder value.
        keep_ok = keep_value >= MIN_SPLIT_VALUE

        # Used ONLY to pick the correct reason tag when keep_ok is False.
        # This intentionally still compares against MIN_SPLIT_VALUE (the
        # original $150), not the new lowered hold threshold — this is
        # diagnostic labeling, not a decision gate, and its meaning did not
        # change.
        bo_ok_at_keep_threshold = bo_value >= MIN_SPLIT_VALUE

        # NEW: once keep_ok is True, this lowered threshold is what actually
        # decides whether a split is attempted at all.
        bo_hold_ok = bo_value >= MIN_BACKORDER_HOLD_VALUE

        logger.info(
            "%s: projected keep=%s (ok=%s @ $%s) backorder=%s (hold_ok=%s @ $%s)",
            name, keep_value, keep_ok, MIN_SPLIT_VALUE, bo_value, bo_hold_ok, MIN_BACKORDER_HOLD_VALUE,
        )

        if not keep_ok:
            tag = pick_minvalue_tag(keep_ok, bo_ok_at_keep_threshold)
            logger.info("%s: ships-now value below $%s, no split attempted. Tagging '%s'.", name, MIN_SPLIT_VALUE, tag)
            final_tags = with_tag(with_tag(without_tag(list(live.get("tags") or []), PROCESSING_TAG), tag), EVAL_DONE_TAG)
            draft_update_return(draft_id, {"tags": final_tags}, label=f"tag {tag}")
            processing_released = True
            return "processed"

        if not bo_hold_ok:
            logger.info(
                "%s: backorder value $%s below $%s hold threshold, no split attempted. Tagging '%s'.",
                name, bo_value, MIN_BACKORDER_HOLD_VALUE, BO_MINVALUE_TAG,
            )
            final_tags = with_tag(with_tag(without_tag(list(live.get("tags") or []), PROCESSING_TAG), BO_MINVALUE_TAG), EVAL_DONE_TAG)
            draft_update_return(draft_id, {"tags": final_tags}, label=f"tag {BO_MINVALUE_TAG}")
            processing_released = True
            return "processed"

        # --- both gates cleared: attempt the real split ---
        existing_po_meta = (live.get("po_meta") or {}).get("value")
        base_po = (existing_po_meta or live.get("poNumber") or "").strip()
        original_lines = list(lines)
        original_tags = list(live.get("tags") or [])
        original_custom_attributes = live.get("customAttributes") or []
        original_metafields = (live.get("metafields") or {}).get("nodes") or []

        child = draft_duplicate(draft_id)
        try:
            ca_add, mf_add = build_linking_fields(base_po=base_po, original_draft_id=draft_id, is_child=True)
            child_input = {
                "lineItems": [build_line_input(l) for l in backorder_lines],
                "poNumber": build_po_number(base_po),
                "tags": with_tag(without_tags(list(original_tags), CONVERSION_TRIGGER_TAGS), BACKORDER_CHILD_TAG),
                "customAttributes": merge_custom_attributes(original_custom_attributes, ca_add),
                "metafields": merge_metafields(original_metafields, mf_add),
            }
            child = draft_update_return(child["id"], child_input, label="child (backorder) update")[1] or child

            parent_input = {"lineItems": [build_line_input(l) for l in keep_lines]}
            parent = draft_update_return(draft_id, parent_input, label="parent (ship-now) update")[1]
        except Exception:
            logger.exception("%s: split mutation failed, rolling back child.", name)
            draft_delete(child["id"], label="rollback child after failed update")
            release_processing_lock(draft_id, original_tags if DRY_RUN else list(live.get("tags") or []))
            processing_released = True
            raise

        # --- verify actual totals, not just projected ones ---
        if DRY_RUN:
            actual_keep_value, actual_bo_value = keep_value, bo_value
            actual_keep_ok = keep_ok
            actual_bo_hold_ok = bo_hold_ok
            actual_bo_ok_at_keep_threshold = bo_ok_at_keep_threshold
        else:
            refreshed_parent = fetch_draft_detail(draft_id)
            refreshed_child = fetch_draft_detail(child["id"])
            actual_keep_value = sum_value((refreshed_parent.get("lineItems") or {}).get("nodes") or [])
            actual_bo_value = sum_value((refreshed_child.get("lineItems") or {}).get("nodes") or [])
            actual_keep_ok = actual_keep_value >= MIN_SPLIT_VALUE
            actual_bo_hold_ok = actual_bo_value >= MIN_BACKORDER_HOLD_VALUE
            actual_bo_ok_at_keep_threshold = actual_bo_value >= MIN_SPLIT_VALUE
            logger.info(
                "%s: actual keep=%s (ok=%s @ $%s) backorder=%s (hold_ok=%s @ $%s)",
                name, actual_keep_value, actual_keep_ok, MIN_SPLIT_VALUE,
                actual_bo_value, actual_bo_hold_ok, MIN_BACKORDER_HOLD_VALUE,
            )

        if not actual_keep_ok:
            # Unwind: the projected keep value didn't hold up against the
            # real, Shopify-calculated total. This is the priority gate —
            # it always wins regardless of the backorder value.
            tag = pick_minvalue_tag(False, actual_bo_ok_at_keep_threshold)
            logger.warning("%s: actual ships-now value failed the $%s gate, unwinding. Tagging '%s'.", name, MIN_SPLIT_VALUE, tag)
            draft_delete(child["id"], label="unwind child (actual keep value below threshold)")
            restore_input = {"lineItems": [build_line_input(l) for l in original_lines]}
            draft_update_return(draft_id, restore_input, label="restore parent lines after unwind")
            final_tags = with_tag(with_tag(without_tag(original_tags, PROCESSING_TAG), tag), EVAL_DONE_TAG)
            draft_update_return(draft_id, {"tags": final_tags}, label=f"tag {tag} after unwind")
            processing_released = True
            return "processed"

        if not actual_bo_hold_ok:
            # Unwind: keep side was fine, but the actual backorder value
            # came in below the $75 hold threshold.
            logger.warning(
                "%s: actual backorder value $%s failed the $%s hold threshold, unwinding. Tagging '%s'.",
                name, actual_bo_value, MIN_BACKORDER_HOLD_VALUE, BO_MINVALUE_TAG,
            )
            draft_delete(child["id"], label="unwind child (actual backorder below hold threshold)")
            restore_input = {"lineItems": [build_line_input(l) for l in original_lines]}
            draft_update_return(draft_id, restore_input, label="restore parent lines after unwind")
            final_tags = with_tag(with_tag(without_tag(original_tags, PROCESSING_TAG), BO_MINVALUE_TAG), EVAL_DONE_TAG)
            draft_update_return(draft_id, {"tags": final_tags}, label=f"tag {BO_MINVALUE_TAG} after unwind")
            processing_released = True
            return "processed"

        # --- success: split stands. Tag the child with its value band based
        # on the ACTUAL (post-verification) value, since the projected and
        # actual values can diverge across the $150 line. The original gets
        # only the internal eval-done marker (no business/outcome tag) so
        # it isn't picked up again. ---
        band_tag = pick_split_band_tag(actual_bo_value)
        child_current_tags = list(child.get("tags") or [])
        if band_tag not in child_current_tags:
            child_final_tags = with_tag(child_current_tags, band_tag)
            child = draft_update_return(child["id"], {"tags": child_final_tags}, label=f"tag child {band_tag}")[1] or child

        final_parent_tags = with_tag(without_tag(original_tags, PROCESSING_TAG), EVAL_DONE_TAG)
        draft_update_return(draft_id, {"tags": final_parent_tags}, label="tag eval-done (split succeeded)")
        logger.info("%s: split succeeded (%s, backorder=%s). Backorder child: %s", name, band_tag, actual_bo_value, child.get("name") or child.get("id"))
        processing_released = True
        return "processed"

    finally:
        if not processing_released:
            try:
                release_processing_lock(draft_id, list(draft.get("tags") or []))
            except Exception:
                logger.exception("%s: CRITICAL — could not release processing lock in final cleanup.", name)


# ----------------------------
# MAIN
# ----------------------------
def chunk_list(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def build_open_ended_query() -> str:
    # Allow-list first: only drafts explicitly marked as belonging to the
    # new pipeline are ever candidates. Everything else below is a
    # defensive secondary filter, not the primary safeguard.
    parts = [
        "status:open",
        f"tag:{ORDER_FLOW_TAG}",
        f"-tag:{EVAL_DONE_TAG}",
        f"-tag:{BACKORDER_CHILD_TAG}",
        f"-tag:{NEEDS_REVIEW_TAG}",
        f"-tag:{PROCESSING_TAG}",
    ]
    if LOOKBACK_DAYS > 0:
        since = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=LOOKBACK_DAYS)).date().isoformat()
        parts.append(f"updated_at:>={since}")
    return " ".join(parts)


def main() -> None:
    targets = {normalize_draft_name(n) for n in DRAFT_ORDER_NAMES} if DRAFT_ORDER_NAMES else set()
    collected: List[Dict[str, Any]] = []
    scanned = 0

    if DRAFT_ORDER_NAMES:
        for chunk in chunk_list(DRAFT_ORDER_NAMES, 12):
            name_query = build_draft_name_query(chunk)
            # Even in scoped test mode, still require the allow-list tag —
            # this is what makes weekend testing safe against legacy drafts.
            query = f"status:open tag:{ORDER_FLOW_TAG} ({name_query})" if name_query else f"status:open tag:{ORDER_FLOW_TAG}"
            after = None
            while True:
                resp = gql(QUERY_DRAFTS, {"first": 250, "after": after, "query": query}).get("draftOrders") or {}
                edges = resp.get("edges") or []
                if not edges:
                    break
                for e in edges:
                    node = e.get("node") or {}
                    if node:
                        collected.append(node)
                        scanned += 1
                page_info = resp.get("pageInfo") or {}
                after = page_info.get("endCursor")
                if not page_info.get("hasNextPage"):
                    break
    else:
        query = build_open_ended_query()
        logger.info("Open-ended query: %s", query)
        page_size = min(250, MAX_DRAFTS)
        after = None
        while True:
            resp = gql(QUERY_DRAFTS, {"first": page_size, "after": after, "query": query}).get("draftOrders") or {}
            edges = resp.get("edges") or []
            if not edges:
                break
            for e in edges:
                node = e.get("node") or {}
                if node:
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

    logger.info("Found %s draft(s) after filtering. DRY_RUN=%s (scanned %s rows)", len(drafts), DRY_RUN, scanned)

    successes, skipped, failed = [], [], []
    for d in drafts:
        draft_name = d.get("name", d.get("id", "(unknown)"))
        try:
            status = process_draft(d["id"])
            (skipped if status == "skipped" else successes).append(draft_name)
        except Exception as e:
            failed.append((draft_name, str(e)))
            logger.error("%s: FAILED — %s", draft_name, e)

    logger.info("")
    logger.info("Run summary")
    logger.info("SUCCESS: %s", len(successes))
    if successes:
        logger.info("  %s", ", ".join(successes))
    logger.info("SKIPPED: %s", len(skipped))
    logger.info("FAILED: %s", len(failed))
    if failed:
        for draft_name, err in failed:
            logger.info("  %s: %s", draft_name, err)


if __name__ == "__main__":
    main()
