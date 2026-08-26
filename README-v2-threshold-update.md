# shopify-adjust-orders-v2.py — $75/$150 threshold update

This covers what changed in the script, the new environment variables you
need to add, and what (eventually) needs to change in the GitHub Actions
workflow file.

---

## 1. What changed in the logic

The "ships now" (keep) side gate is **unchanged** — it still has to clear
$150 (`MIN_SPLIT_VALUE`), and it's still the priority check. If it fails,
the order is never split, no matter what the backorder is worth.

The backorder side used to need $150 too, or the whole order got unwound.
Now it only needs **$75** (`MIN_BACKORDER_HOLD_VALUE`) to trigger a split.
Once it does split, the backorder child gets an extra tag depending on
which side of $150 its actual value lands on:

| Keep value | Backorder value | Result | Backorder child tags |
|---|---|---|---|
| < $150 | any | Unwind | `instock-minvalue` or `order-minvalue` (unchanged logic) |
| ≥ $150 | < $75 | Unwind | `BO-minvalue` (unchanged tag, new $75 threshold) |
| ≥ $150 | $75 – $149.99 | **Split** | `split1` + `split-remainder` |
| ≥ $150 | ≥ $150 | **Split** | `split1` + `split-150` |

The value-band tag (`split-remainder` vs `split-150`) is assigned **after**
the create-then-verify step, using the actual Shopify-calculated backorder
total — not the projected one — since the two can land on different sides
of $150.

---

## 2. New environment variables to add

Add these as GitHub Actions **repository variables** (`vars.*`), same tier
as `MIN_SPLIT_VALUE` presumably lives today (double check — it's not
currently listed in the workflow's env block below, so if it isn't set as
a repo variable yet, this is a good time to add it alongside these three):

| Variable | Default if unset | Purpose |
|---|---|---|
| `MIN_SPLIT_VALUE` | `150` | Keep-side gate (unchanged, but confirm it's actually set as a repo variable) |
| `MIN_BACKORDER_HOLD_VALUE` | `75` | **New.** Backorder-side gate that decides whether a split is attempted at all |
| `SPLIT_REMAINDER_TAG` | `split-remainder` | **New.** Tag for splits where the backorder lands in the $75–$150 band |
| `SPLIT_150_TAG` | `split-150` | **New.** Tag for splits where the backorder is ≥ $150 |

All four have safe defaults baked into the script, so nothing breaks if
you forget to set them — but you'll want them explicit and GitHub-editable
per your existing pattern for `MIN_SPLIT_VALUE`.

---

