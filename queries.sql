-- ============================================================
-- ruck-bulk-categorizer — Inspection Queries
-- ============================================================
-- Connect via SSM tunnel before running (see README.md).
-- All queries are read-only SELECT statements.
--
-- INDEX
-- ─────
--  1.  Category tree (full hierarchy with depth)
--  2.  Leaf categories only (with item + attribute counts)
--  3.  Parent/non-leaf categories only
--  4.  Items per leaf category (sorted by count)
--  5.  Items with their assigned categories
--  6.  Items with NO category assigned
--  7.  Items with categories AND their attribute values
--  8.  Items missing all attribute values (have category, no attrs)
--  9.  All attributes per category (including inherited)
-- 10.  Attribute units
-- 11.  Category → attributes → units (full taxonomy detail)
-- 12.  Attribute value distribution for a single attribute key
-- 13.  Most common attribute values across all items
-- 14.  Coverage: how many items have each attribute filled in
-- 15.  Items with a specific category path
-- 16.  Search items by title keyword
-- 17.  Summary dashboard (counts of everything)
-- ============================================================


-- ============================================================
-- Shared CTE: full category path (used in several queries)
-- ============================================================
-- Paste this WITH block at the top of any query that needs it.
--
-- WITH RECURSIVE cat_path AS (
--     SELECT id, name, slug, parent_id,
--            name::text AS full_path,
--            slug::text AS slug_path,
--            0          AS depth
--     FROM marketplace_categories
--     WHERE parent_id IS NULL AND deleted_at IS NULL
--     UNION ALL
--     SELECT c.id, c.name, c.slug, c.parent_id,
--            cp.full_path || ' > ' || c.name,
--            cp.slug_path || '/' || c.slug,
--            cp.depth + 1
--     FROM marketplace_categories c
--     JOIN cat_path cp ON cp.id = c.parent_id
--     WHERE c.deleted_at IS NULL
-- )


-- ============================================================
-- 1. Category tree — full hierarchy with depth
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name, slug, parent_id,
           name::text AS full_path,
           slug::text AS slug_path,
           0          AS depth
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, c.name, c.slug, c.parent_id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           cp.depth + 1
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    repeat('  ', depth) || name  AS indented_name,
    slug_path,
    depth
FROM cat_path
ORDER BY slug_path;


-- ============================================================
-- 2. Leaf categories only — with item and attribute counts
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name, slug, parent_id,
           name::text AS full_path,
           slug::text AS slug_path,
           0          AS depth
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, c.name, c.slug, c.parent_id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           cp.depth + 1
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
),
leaf_check AS (
    SELECT DISTINCT parent_id AS id
    FROM marketplace_categories
    WHERE parent_id IS NOT NULL AND deleted_at IS NULL
)
SELECT
    cp.slug_path,
    cp.full_path,
    cp.depth,
    COUNT(DISTINCT mic.item_id)  AS item_count,
    COUNT(DISTINCT ma.id)        AS attribute_count
FROM cat_path cp
LEFT JOIN leaf_check lc    ON lc.id = cp.id
LEFT JOIN marketplace_item_categories mic
    ON mic.category_id = cp.id AND mic.deleted_at IS NULL
LEFT JOIN marketplace_attributes ma
    ON ma.category_id = cp.id AND ma.deleted_at IS NULL
WHERE lc.id IS NULL
GROUP BY cp.slug_path, cp.full_path, cp.depth
ORDER BY item_count DESC, cp.slug_path;


-- ============================================================
-- 3. Parent/non-leaf categories only
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name, slug, parent_id,
           name::text AS full_path,
           slug::text AS slug_path,
           0          AS depth
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, c.name, c.slug, c.parent_id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           cp.depth + 1
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
),
leaf_check AS (
    SELECT DISTINCT parent_id AS id
    FROM marketplace_categories
    WHERE parent_id IS NOT NULL AND deleted_at IS NULL
)
SELECT
    cp.slug_path,
    cp.full_path,
    cp.depth,
    COUNT(DISTINCT ma.id) AS attribute_count
FROM cat_path cp
JOIN leaf_check lc ON lc.id = cp.id
LEFT JOIN marketplace_attributes ma
    ON ma.category_id = cp.id AND ma.deleted_at IS NULL
GROUP BY cp.slug_path, cp.full_path, cp.depth
ORDER BY cp.slug_path;


-- ============================================================
-- 4. Items per leaf category — sorted by count
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name::text AS full_path, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    cp.slug_path,
    cp.full_path,
    COUNT(mic.item_id) AS item_count
FROM marketplace_item_categories mic
JOIN cat_path cp ON cp.id = mic.category_id
WHERE mic.deleted_at IS NULL
GROUP BY cp.slug_path, cp.full_path
ORDER BY item_count DESC;


-- ============================================================
-- 5. Items with their assigned categories
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name::text AS full_path, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    i.id           AS item_id,
    i.title        AS item_name,
    cp.slug_path   AS category_path,
    cp.full_path   AS category_full
FROM items i
JOIN marketplace_item_categories mic
    ON mic.item_id = i.id AND mic.deleted_at IS NULL
JOIN cat_path cp ON cp.id = mic.category_id
WHERE i.deleted_at IS NULL
ORDER BY cp.slug_path, i.title
LIMIT 500;


-- ============================================================
-- 6. Items with NO category assigned
-- ============================================================
SELECT
    i.id    AS item_id,
    i.title AS item_name,
    i.category    AS legacy_category,
    i.subcategory AS legacy_subcategory
FROM items i
WHERE i.deleted_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM marketplace_item_categories mic
      WHERE mic.item_id = i.id AND mic.deleted_at IS NULL
  )
ORDER BY i.title
LIMIT 500;


-- ============================================================
-- 7. Items with categories AND their attribute values
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, cp.slug_path || '/' || c.slug, c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    i.id                AS item_id,
    i.title             AS item_name,
    cp.slug_path        AS category_path,
    ma.key              AS attribute_key,
    ma.label            AS attribute_label,
    mav.value,
    mau.symbol          AS unit
FROM items i
JOIN marketplace_item_categories mic
    ON mic.item_id = i.id AND mic.deleted_at IS NULL
JOIN cat_path cp ON cp.id = mic.category_id
LEFT JOIN marketplace_attribute_values mav
    ON mav.item_id = i.id AND mav.deleted_at IS NULL
LEFT JOIN marketplace_attributes ma
    ON ma.id = mav.attribute_id AND ma.deleted_at IS NULL
LEFT JOIN marketplace_attribute_units mau
    ON mau.id = mav.attribute_unit_id AND mau.deleted_at IS NULL
WHERE i.deleted_at IS NULL
ORDER BY i.id, ma.key
LIMIT 500;


-- ============================================================
-- 8. Items missing ALL attribute values (have category, no attrs)
-- ============================================================
SELECT
    i.id    AS item_id,
    i.title AS item_name
FROM items i
JOIN marketplace_item_categories mic
    ON mic.item_id = i.id AND mic.deleted_at IS NULL
WHERE i.deleted_at IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM marketplace_attribute_values mav
      WHERE mav.item_id = i.id AND mav.deleted_at IS NULL
  )
ORDER BY i.title
LIMIT 500;


-- ============================================================
-- 9. All attributes per category (direct only, not inherited)
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name::text AS full_path, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    cp.slug_path        AS category_path,
    cp.full_path        AS category_full,
    ma.key,
    ma.label,
    ma.description,
    mau.symbol          AS unit_symbol,
    mau.name            AS unit_name
FROM marketplace_attributes ma
JOIN cat_path cp ON cp.id = ma.category_id
LEFT JOIN marketplace_attribute_units mau
    ON mau.id = ma.unit_required AND mau.deleted_at IS NULL
WHERE ma.deleted_at IS NULL
ORDER BY cp.slug_path, ma.key;


-- ============================================================
-- 10. Attribute units — full list
-- ============================================================
SELECT
    symbol,
    name,
    description,
    value_type
FROM marketplace_attribute_units
WHERE deleted_at IS NULL
ORDER BY symbol;


-- ============================================================
-- 11. Category → attributes → units (full taxonomy detail)
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, name::text AS full_path, slug::text AS slug_path, parent_id, 0 AS depth
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id,
           cp.full_path || ' > ' || c.name,
           cp.slug_path || '/' || c.slug,
           c.parent_id,
           cp.depth + 1
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    cp.slug_path                         AS category_path,
    cp.depth,
    ma.key                               AS attr_key,
    ma.label                             AS attr_label,
    mau.symbol                           AS unit_symbol,
    mau.value_type
FROM cat_path cp
JOIN marketplace_attributes ma ON ma.category_id = cp.id AND ma.deleted_at IS NULL
LEFT JOIN marketplace_attribute_units mau ON mau.id = ma.unit_required AND mau.deleted_at IS NULL
ORDER BY cp.slug_path, ma.key;


-- ============================================================
-- 12. Attribute value distribution for a single attribute key
--     (change 'width' to the key you want to inspect)
-- ============================================================
SELECT
    mav.value,
    mau.symbol      AS unit,
    COUNT(*)        AS occurrences
FROM marketplace_attribute_values mav
JOIN marketplace_attributes ma
    ON ma.id = mav.attribute_id AND ma.key = 'width'
LEFT JOIN marketplace_attribute_units mau
    ON mau.id = mav.attribute_unit_id
WHERE mav.deleted_at IS NULL
GROUP BY mav.value, mau.symbol
ORDER BY occurrences DESC
LIMIT 100;


-- ============================================================
-- 13. Most common attribute values across ALL items
-- ============================================================
SELECT
    ma.key              AS attribute_key,
    mav.value,
    mau.symbol          AS unit,
    COUNT(*)            AS occurrences
FROM marketplace_attribute_values mav
JOIN marketplace_attributes ma
    ON ma.id = mav.attribute_id AND ma.deleted_at IS NULL
LEFT JOIN marketplace_attribute_units mau
    ON mau.id = mav.attribute_unit_id
WHERE mav.deleted_at IS NULL
GROUP BY ma.key, mav.value, mau.symbol
ORDER BY occurrences DESC
LIMIT 100;


-- ============================================================
-- 14. Coverage: how many items have each attribute filled in
--     (useful for spotting attributes with poor extraction)
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, cp.slug_path || '/' || c.slug, c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    cp.slug_path        AS category_path,
    ma.key              AS attribute_key,
    COUNT(DISTINCT mic.item_id)  AS items_in_category,
    COUNT(DISTINCT mav.item_id)  AS items_with_value,
    ROUND(
        100.0 * COUNT(DISTINCT mav.item_id)
        / NULLIF(COUNT(DISTINCT mic.item_id), 0), 1
    )                            AS coverage_pct
FROM marketplace_attributes ma
JOIN cat_path cp ON cp.id = ma.category_id
LEFT JOIN marketplace_item_categories mic
    ON mic.category_id = ma.category_id AND mic.deleted_at IS NULL
LEFT JOIN marketplace_attribute_values mav
    ON mav.attribute_id = ma.id AND mav.deleted_at IS NULL
WHERE ma.deleted_at IS NULL
GROUP BY cp.slug_path, ma.key
ORDER BY coverage_pct ASC, cp.slug_path, ma.key;


-- ============================================================
-- 15. Items with a specific category path
--     (change 'pressure_treated_lumber/pt_ground_contact')
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, cp.slug_path || '/' || c.slug, c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    i.id        AS item_id,
    i.title     AS item_name,
    i.description
FROM items i
JOIN marketplace_item_categories mic ON mic.item_id = i.id AND mic.deleted_at IS NULL
JOIN cat_path cp ON cp.id = mic.category_id
WHERE i.deleted_at IS NULL
  AND cp.slug_path = 'pressure_treated_lumber/pt_ground_contact'
ORDER BY i.title;


-- ============================================================
-- 16. Search items by title keyword
--     (change '%cedar%' to the keyword you want)
-- ============================================================
WITH RECURSIVE cat_path AS (
    SELECT id, slug::text AS slug_path, parent_id
    FROM marketplace_categories
    WHERE parent_id IS NULL AND deleted_at IS NULL
    UNION ALL
    SELECT c.id, cp.slug_path || '/' || c.slug, c.parent_id
    FROM marketplace_categories c
    JOIN cat_path cp ON cp.id = c.parent_id
    WHERE c.deleted_at IS NULL
)
SELECT
    i.id            AS item_id,
    i.title         AS item_name,
    cp.slug_path    AS category_path
FROM items i
LEFT JOIN marketplace_item_categories mic
    ON mic.item_id = i.id AND mic.deleted_at IS NULL
LEFT JOIN cat_path cp ON cp.id = mic.category_id
WHERE i.deleted_at IS NULL
  AND i.title ILIKE '%cedar%'
ORDER BY i.title
LIMIT 200;


-- ============================================================
-- 17. Summary dashboard — counts of everything
-- ============================================================
SELECT
    (SELECT COUNT(*) FROM items
        WHERE deleted_at IS NULL)                                      AS total_items,
    (SELECT COUNT(*) FROM marketplace_categories
        WHERE deleted_at IS NULL)                                      AS total_categories,
    (SELECT COUNT(*) FROM marketplace_attributes
        WHERE deleted_at IS NULL)                                      AS total_attributes,
    (SELECT COUNT(*) FROM marketplace_attribute_units
        WHERE deleted_at IS NULL)                                      AS total_units,
    (SELECT COUNT(DISTINCT item_id) FROM marketplace_item_categories
        WHERE deleted_at IS NULL)                                      AS items_categorized,
    (SELECT COUNT(*) FROM items WHERE deleted_at IS NULL)
    - (SELECT COUNT(DISTINCT item_id) FROM marketplace_item_categories
        WHERE deleted_at IS NULL)                                      AS items_uncategorized,
    (SELECT COUNT(DISTINCT item_id) FROM marketplace_attribute_values
        WHERE deleted_at IS NULL)                                      AS items_with_attrs,
    (SELECT COUNT(*) FROM marketplace_attribute_values
        WHERE deleted_at IS NULL)                                      AS total_attribute_values;
