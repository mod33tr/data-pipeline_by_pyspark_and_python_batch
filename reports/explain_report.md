# 📊 Explain Report (Before / After Indexes)

## 🔑 Indexes Created
- **`idx_city_status`** `[('city', 1), ('status', 1)]` — Compound Index: يطابق فلتر (مدينة+حالة) الأكثر استخداماً ويخدم orders_by_city_status و city_status_count
- **`idx_customer_id`** `[('customer_id', 1)]` — Single Index: بحث سريع عن تاريخ عميل معين بدون فحص كامل للمجموعة
- **`idx_date_payment`** `[('order_date', -1), ('payment_status', 1)]` — Compound Index: نطاقات التاريخ + فلتر الدفع؛ يخدم orders_in_date_range و paid_orders_in_date_range

## 📈 Performance Comparison

| Query | Before (docs examined) | After (docs examined) | Speed-up |
|-------|----------------------|---------------------|----------|
| `orders_by_city_status` | 26,461,560 (COLLSCAN) | 2,920 (IXSCAN) | **9062x** |
| `customer_order_history` | 26,461,560 (COLLSCAN) | 1 (IXSCAN) | **26461560x** |
| `paid_orders_in_date_range` | 26,461,560 (COLLSCAN) | 50 (IXSCAN) | **529231x** |

## 📝 Detailed Results

### `orders_by_city_status`
- **Before:** docsExamined=26,461,560, stages=['PROJECTION_SIMPLE', 'SORT', 'COLLSCAN'], time=64463ms
- **After :** docsExamined=2,920, stages=['LIMIT', 'PROJECTION_SIMPLE', 'FETCH', 'IXSCAN'], time=1000ms

### `customer_order_history`
- **Before:** docsExamined=26,461,560, stages=['PROJECTION_SIMPLE', 'SORT', 'COLLSCAN'], time=50814ms
- **After :** docsExamined=1, stages=['PROJECTION_SIMPLE', 'SORT', 'FETCH', 'IXSCAN'], time=2ms

### `paid_orders_in_date_range`
- **Before:** docsExamined=26,461,560, stages=['PROJECTION_SIMPLE', 'SORT', 'COLLSCAN'], time=61043ms
- **After :** docsExamined=50, stages=['LIMIT', 'PROJECTION_SIMPLE', 'FETCH', 'IXSCAN'], time=3ms
