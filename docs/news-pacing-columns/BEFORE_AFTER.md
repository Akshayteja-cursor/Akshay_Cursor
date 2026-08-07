# News Pacing Report — Before vs After (AOS Columns)

## Field source map (by sheet)

| Column | News / Sports GAM | FW PG (Freewheel) | Amperwave | Spotify (Megaphone) |
|---|---|---|---|---|
| Advertiser | Ad server + AOS join | AdOps lifetime / AOS | **gold table** (AOS-joined) | **gold table** (AOS-joined) |
| AOS Deal ID | **AOS** `Sales Order ID` via `Operative_OMS` | **AOS** same | **table** `deal_id` | **table** `deal_id` |
| Ad Server Deal Name | GAM Order / Campaign Name | FW Campaign / Order | **table** `deal_name` | **table** `deal_name` |
| AOS Deal Line ID | **AOS** `Sales Line Item ID` | **AOS** same | **table** `deal_line_item_id` | **table** `deal_line_item_id` |
| External Ad ID | **AOS** `Placement ID` / PS Line Item ID | **AOS** same | **table** `external_ad_id` | **table** `external_ad_id` |
| Ad Server Line Item Name | GAM / FW line name | FW line name | **table** `deal_line_item_name` | **table** `deal_line_item_name` |
| Line Item Type | **GAM only** (`LINE_ITEM_TYPE`) | blank | blank | blank |
| External System | **AOS** `Production System Name` (fallback Instance) | **AOS** / Freewheel | set to **Amperwave** | set to **Megaphone** |
| Billable Third Party Server | **AOS** | **AOS** | **table** `billable_third_party_descr` | **table** `billable_third_party_descr` |
| Campaign Manager | **AOS** `Trafficker/Campaign Manager` | **AOS** | blank today (not on gold table) | blank today |
| Delivery metrics | GAM / Adjuster / FW | FW + Adjuster | **table** `delivered_impressions` | **table** `delivered_impressions` |

Tables used for podcast sheets:
- `ft_amperwave_campaign_delivery` (widget `amperwave_delta_table`)
- `ft_megaphone_campaign_delivery` (widget `megaphone_delta_table`)

Those gold tables already left-join AOS (`ft_digital_operative_line_date_allocation`).

---

## Column order

### BEFORE
```
Advertiser
Order
Line Item Name
Line Item Start Date
Line Item End Date
Rate
Goal Quantity
Contracted Quantity
Delivery Indicator
Salesperson
Ad Server Impressions
Impressions (3rd Party)
Clicks (3rd Party)
3rd Party CTR
Buffer
Discrepancy
Current First Party OSI
Current Third Party OSI
Total Error Count
Total Error Rate
```

### AFTER
```
Advertiser
AOS Deal ID
Ad Server Deal Name          # was Order
AOS Deal Line ID
External Ad ID
Ad Server Line Item Name     # was Line Item Name
Line Item Type               # GAM only
External System
Line Item Start Date
Line Item End Date
Billable Third Party Server
Rate
Goal Quantity
Delivery Indicator
Salesperson
Campaign Manager
Contracted Quantity          # rest unchanged
Ad Server Impressions
Impressions (3rd Party)
Clicks (3rd Party)
3rd Party CTR
Buffer
Discrepancy
Current First Party OSI
Current Third Party OSI
Total Error Count
Total Error Rate
```

---

## Code before vs after (DEV `format_news_pacing`)

### BEFORE
```python
df = df.loc[:,
            [
                'Instance',
                'Advertiser',
                'Order',
                'Line Item Name',
                'Line Item Start Date',
                'Line Item End Date',
                'Rate',
                'Goal Quantity',
                'Contracted Quantity',
                'Delivery Indicator',
                'Primary Salesperson Full Name',
                'Ad Server Impressions',
                ...
            ]]
df = df.rename(columns={'Primary Salesperson Full Name': 'Salesperson'})
```

### AFTER
```python
# Map AOS fields from Operative_OMS merge
df['AOS Deal ID'] = df['Sales Order ID']
df['AOS Deal Line ID'] = df['Sales Line Item ID']
df['External Ad ID'] = df['Placement ID']
df['Campaign Manager'] = df['Trafficker/Campaign Manager']
df['External System'] = normalize_external_system(Production System Name, Instance)
df = df.rename(columns={
    'Order': 'Ad Server Deal Name',
    'Line Item Name': 'Ad Server Line Item Name',
})
df = df.loc[:, REPORT_COLUMNS]
```

### Podcast tables (Amperwave / Spotify) — AFTER mapping
```python
PODCAST_COLUMN_MAP = {
    'deal_id': 'AOS Deal ID',
    'deal_name': 'Ad Server Deal Name',
    'deal_line_item_id': 'AOS Deal Line ID',
    'external_ad_id': 'External Ad ID',
    'deal_line_item_name': 'Ad Server Line Item Name',
    'billable_third_party_descr': 'Billable Third Party Server',
    ...
}
df['External System'] = normalize_external_system(None, instance_label)  # Amperwave / Megaphone
df['Line Item Type'] = ''  # GAM-only
```
