## Database Schema Design

### Overview
The database schema was designed to enforce data integrity, ensure referential consistency, and align with pipeline validation rules.

---

### 1. Customers Table

- `customer_id` is the primary key.
- `email` is stored in lowercase to ensure consistency.
- A CHECK constraint (`email = LOWER(email)`) enforces normalization at the database level.
- A UNIQUE constraint is applied on `email` to prevent duplicates.
- `signup_date` and `is_active` are marked NOT NULL as they are required for analytics and filtering.
- `country_code` is optional due to missing values in source data.

---

### 2. Orders Table

- `order_id` is the primary key.
- `customer_id` is a foreign key referencing `customers(customer_id)` to enforce referential integrity.
- `order_ts` uses `TIMESTAMPTZ` to support timezone-aware timestamps.
- A CHECK constraint enforces valid status values:
  ('placed', 'shipped', 'cancelled', 'refunded').
- All core fields (`customer_id`, `order_ts`, `status`, `total_amount`, `currency`) are NOT NULL to ensure reliability.

---

### 3. Order Items Table

- Composite primary key (`order_id`, `line_no`) ensures uniqueness per order line.
- `order_id` is a foreign key referencing `orders(order_id)`.
- CHECK constraints enforce:
  - `quantity > 0`
  - `unit_price > 0`
- These constraints prevent invalid transactional data from being stored.

---

### Design Decisions & Trade-offs

- Email normalization is enforced both in ETL and at the database level for safety.
- Invalid data (e.g., bad status, missing foreign keys, non-positive values) is filtered during ETL rather than stored.
- Constraints act as a second layer of defense to guarantee data quality.
- The schema is designed to be simple, enforceable, and aligned with analytical use cases.

---

### Repeatability

The schema is created using a repeatable SQL script (`schema.sql`) that drops and recreates tables, allowing consistent environment setup across runs.