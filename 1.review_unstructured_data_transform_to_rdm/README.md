# Q1. Review Existing Unstructured Data and Diagram a New Structured Relational Data Model (Rationale for Data Modeling Decisions)

## 1. **Reason for Keeping `points_payer_id` Separate:**

The `points_payer_id` field is intended to represent the entity responsible for awarding or paying points for a specific item on a receipt. This could be a user, a brand, a third-party partner, or another entity within the rewards or loyalty program.

### Potential Scenarios and Implications:

- **User as Points Payer:**

  - **Scenario:** Users can award points to each other or redeem points for purchases.
  - **Implication:** `points_payer_id` should reference the `Users` table.

- **Brand or Partner as Points Payer:**

  - **Scenario:** Points are awarded by brands or partners involved in the loyalty program.
  - **Implication:** `points_payer_id` should reference a table representing brands or partners, such as the `Brands` table.

- **Generic Entity as Points Payer:**
  - **Scenario:** Points could be awarded by various types of entities (e.g., users, brands, external partners).
  - **Implication:** The database schema should be flexible enough to accommodate multiple entity types as points payers. This may involve a more versatile structure to handle different payer entities effectively.

## 2. **Normalization:**

- **Rationale:** The database design follows normalization principles to reduce data redundancy and ensure data integrity. Each table represents a distinct entity, and relationships are managed through foreign keys.

## 3. **Indexing:**

- **Rationale:** Indexes should created on frequently queried columns (e.g., `created_date` on `users`, `purchase_date` on `receipts`, `rewards_receipt_status` on `receipts`, `brand_id` on `receipt_items`) to enhance query performance and retrieval speed.

## 4. **Date and Time Handling:**

- **Rationale:** Using `TIMESTAMP` data types for `created_date`, `purchase_date`, and other date-related fields ensures accurate and efficient date operations, comparisons, and queries.
