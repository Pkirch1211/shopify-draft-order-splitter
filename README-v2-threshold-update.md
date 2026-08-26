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

## 3. What needs to change in the `.yml` workflow

Two things, whenever you're ready to cut over:

**a) The four variables above need to be threaded through as `env:`** in
the "Run draft splitter" step, the same way `MIN_SPLIT_VALUE` should be
(pattern already used for `PROCESSING_TAG`, `CHILD_TAG`, etc.):

```yaml
MIN_SPLIT_VALUE: ${{ vars.MIN_SPLIT_VALUE }}
MIN_BACKORDER_HOLD_VALUE: ${{ vars.MIN_BACKORDER_HOLD_VALUE }}
SPLIT_REMAINDER_TAG: ${{ vars.SPLIT_REMAINDER_TAG }}
SPLIT_150_TAG: ${{ vars.SPLIT_150_TAG }}
```

**b) The workflow currently runs the OLD script**, not this one:

```yaml
- name: Run draft splitter
  ...
  run: |
    python shopify-adjust-orders.py
```

This attached workflow (`Draft Order Splitter`, `*/30 * * * *` cron) is
still pointed at `shopify-adjust-orders.py`, the legacy v1 script — not
`shopify-adjust-orders-v2.py`. That's presumably intentional until v2 is
fully signed off, but flagging it since it's the actual cutover point:
until this line changes, none of the v2 logic (allow-list tag, dual gate,
now this $75/$150 banding) runs in production at all. When you're ready to
go live, this becomes:

```yaml
    python shopify-adjust-orders-v2.py
```

I haven't made that change myself — didn't want to flip the live cutover
without you explicitly saying so, given the design-first approach you've
been taking on this whole project.

Also worth double-checking: this workflow doesn't currently pass
`ORDER_FLOW_TAG`, `V2_PROCESSING_TAG`, `V2_EVAL_DONE_TAG`,
`BACKORDER_CHILD_TAG`, `INSTOCK_MINVALUE_TAG`, `BO_MINVALUE_TAG`, or
`ORDER_MINVALUE_TAG` either — all of which the v2 script also reads from
env with defaults. Same story: defaults will work, but you'll likely want
them explicit before this goes live on a cron.
