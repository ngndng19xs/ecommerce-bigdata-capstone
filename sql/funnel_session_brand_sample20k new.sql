-- =====================================================================
-- TRUY VẤN SPARK SQL MẪU: FUNNEL / SESSION / BRAND  (chạy trên sample_20k.csv)
-- Bám theo: "Đặc tả kỹ thuật: Thống nhất định nghĩa funnel TMĐT" (Tuần 1 - T004)
-- Vai trò : TV2 (Big Data Analytics)
-- Môi trường: Spark 3.1.2, spark-sql, chế độ local
-- Quy ước : UTC | đơn vị đếm là user_session | funnel TUẦN TỰ là chính thức
--           so sánh thời gian dùng >= | brand và category thiếu -> 'unknown'
-- Lưu ý   : file này CHƯA được chạy thử trên Spark. Các số "THAM CHIẾU" ở cuối
--           file được tính bằng SQLite với cùng logic, dùng để đối chiếu.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 0. CẤU HÌNH
-- ---------------------------------------------------------------------
SET spark.sql.session.timeZone = UTC;
SET spark.sql.shuffle.partitions = 4;

-- ---------------------------------------------------------------------
-- 1. ĐỌC DỮ LIỆU MẪU
--    SỬA đường dẫn ở dòng path cho đúng vị trí file trong máy ảo.
--    Phải có tiền tố file:/// vì Spark có thể đang mặc định đọc từ HDFS.
--    Khai báo schema tường minh nên dòng tiêu đề được bỏ qua (kể cả ký tự BOM).
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW events_raw (
  event_time    STRING,
  event_type    STRING,
  product_id    STRING,
  category_id   STRING,
  category_code STRING,
  brand         STRING,
  price         DOUBLE,
  user_id       STRING,
  user_session  STRING
) USING csv
OPTIONS (path 'file:///home/student/sample_20k.csv', header 'true');

-- Chuyển event_time từ chuỗi dạng "2019-11-01 00:00:00 UTC" sang TIMESTAMP (mục 3)
CREATE OR REPLACE TEMPORARY VIEW events_parsed AS
SELECT
  to_timestamp(regexp_replace(event_time, ' UTC$', ''), 'yyyy-MM-dd HH:mm:ss') AS event_time,
  event_time AS event_time_raw,
  event_type, product_id, category_code, brand, price, user_id, user_session
FROM events_raw;

-- ---------------------------------------------------------------------
-- 2. BÁO CÁO CHẤT LƯỢNG DỮ LIỆU (mục 12)
-- ---------------------------------------------------------------------
-- 2.1 Thống kê giá trị thiếu và bất thường
SELECT
  COUNT(*)                                                              AS total_rows,
  SUM(CASE WHEN user_session  IS NULL THEN 1 ELSE 0 END)                AS rows_null_session,
  SUM(CASE WHEN event_type    IS NULL THEN 1 ELSE 0 END)                AS rows_null_event_type,
  SUM(CASE WHEN product_id    IS NULL THEN 1 ELSE 0 END)                AS rows_null_product_id,
  SUM(CASE WHEN brand         IS NULL THEN 1 ELSE 0 END)                AS rows_null_brand,
  SUM(CASE WHEN category_code IS NULL THEN 1 ELSE 0 END)                AS rows_null_category,
  SUM(CASE WHEN event_time_raw IS NOT NULL AND event_time IS NULL THEN 1 ELSE 0 END) AS rows_bad_timestamp,
  SUM(CASE WHEN price <= 0 THEN 1 ELSE 0 END)                           AS rows_price_le_0,
  COUNT(DISTINCT user_session)                                          AS sessions_raw,
  MIN(event_time)                                                       AS min_event_time,
  MAX(event_time)                                                       AS max_event_time
FROM events_parsed;

-- 2.2 Các loại sự kiện xuất hiện trong dữ liệu nguồn
SELECT event_type, COUNT(*) AS n_events
FROM events_parsed GROUP BY event_type ORDER BY n_events DESC;

-- 2.3 Bản ghi trùng hoàn toàn (không loại bỏ, chỉ thống kê)
SELECT COUNT(*) AS dup_groups, COALESCE(SUM(n - 1), 0) AS dup_extra_rows
FROM (
  SELECT COUNT(*) AS n
  FROM events_parsed
  GROUP BY event_time_raw, event_type, product_id, user_id, user_session
  HAVING COUNT(*) > 1
) t;

-- ---------------------------------------------------------------------
-- 3. BẢNG SỰ KIỆN ĐÃ CHUẨN HÓA (mục 11.1)
--    category lấy từ cột nguồn category_code. Thiếu -> 'unknown'.
--    Loại bản ghi thiếu user_session hoặc event_time không chuyển đổi được.
--    Không loại giá <= 0 và không loại bản ghi trùng (đặc tả không yêu cầu).
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW events_clean AS
SELECT
  event_time,
  DATE(event_time)                                        AS event_date,
  user_session,
  user_id,
  event_type,
  product_id,
  COALESCE(category_code, 'unknown')                      AS category,
  COALESCE(brand, 'unknown')                              AS brand,
  price,
  DATE(MIN(event_time) OVER (PARTITION BY user_session))  AS session_date
FROM events_parsed
WHERE user_session IS NOT NULL
  AND event_time   IS NOT NULL;

-- ---------------------------------------------------------------------
-- 4. BẢNG FUNNEL CẤP PHIÊN (mục 11.2) - mỗi phiên 1 dòng
--    fv = first_view_time
--    fc = first_cart_after_view_time     = MIN(cart có event_time >= fv)
--    fp = first_purchase_after_cart_time = MIN(purchase có event_time >= fc)
--    *_ordered : funnel tuần tự (chính thức)
--    has_*     : funnel không thứ tự (đối chiếu)
--    sessions_skip_stage : cờ phiên có View/Cart/Purchase nhưng không đạt thứ tự
--    Các cột n_events, n_view, n_cart, n_purchase, session_start_time,
--    session_end_time, revenue là cột bổ sung cho phân tích phiên.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW session_funnel AS
WITH e1 AS (
  SELECT *,
    MIN(CASE WHEN event_type = 'view' THEN event_time END)
      OVER (PARTITION BY user_session) AS fv
  FROM events_clean
),
e2 AS (
  SELECT *,
    MIN(CASE WHEN event_type = 'cart' AND event_time >= fv THEN event_time END)
      OVER (PARTITION BY user_session) AS fc
  FROM e1
),
e3 AS (
  SELECT *,
    MIN(CASE WHEN event_type = 'purchase' AND event_time >= fc THEN event_time END)
      OVER (PARTITION BY user_session) AS fp
  FROM e2
),
agg AS (
  SELECT
    user_session,
    MIN(session_date)  AS session_date,
    MIN(event_time)    AS session_start_time,
    MAX(event_time)    AS session_end_time,
    MIN(fv)            AS first_view_time,
    MIN(fc)            AS first_cart_after_view_time,
    MIN(fp)            AS first_purchase_after_cart_time,
    COUNT(*)           AS n_events,
    SUM(CASE WHEN event_type = 'view'             THEN 1 ELSE 0 END) AS n_view,
    SUM(CASE WHEN event_type = 'cart'             THEN 1 ELSE 0 END) AS n_cart,
    SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS n_remove,
    SUM(CASE WHEN event_type = 'purchase'         THEN 1 ELSE 0 END) AS n_purchase,
    SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END)     AS revenue
  FROM e3
  GROUP BY user_session
),
flags AS (
  SELECT *,
    (first_view_time IS NOT NULL)                 AS has_view_ordered,
    (first_cart_after_view_time IS NOT NULL)      AS has_cart_ordered,
    (first_purchase_after_cart_time IS NOT NULL)  AS has_purchase_ordered,
    (n_view > 0)                                  AS has_view,
    (n_cart > 0)                                  AS has_cart,
    (n_purchase > 0)                              AS has_purchase
  FROM agg
)
SELECT *,
  ((has_cart     AND NOT has_cart_ordered) OR
   (has_purchase AND NOT has_purchase_ordered))   AS sessions_skip_stage
FROM flags;

-- Xem thử 10 phiên có cả Cart và Purchase
SELECT user_session, first_view_time, first_cart_after_view_time,
       first_purchase_after_cart_time, has_view_ordered, has_cart_ordered,
       has_purchase_ordered, n_events
FROM session_funnel
WHERE has_cart AND has_purchase
ORDER BY n_events DESC
LIMIT 10;

-- ---------------------------------------------------------------------
-- 5. FUNNEL
-- 5.1 Bảng tổng hợp theo ngày (mục 11.3), dùng session_date
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY VIEW funnel_counts AS
SELECT session_date,
  SUM(CAST(has_view_ordered     AS INT)) AS S_view,
  SUM(CAST(has_cart_ordered     AS INT)) AS S_cart,
  SUM(CAST(has_purchase_ordered AS INT)) AS S_purchase,
  SUM(CAST(sessions_skip_stage  AS INT)) AS sessions_skip_stage,
  SUM(n_remove)                          AS n_remove
FROM session_funnel
GROUP BY session_date;

SELECT session_date, S_view, S_cart, S_purchase,
  ROUND(S_cart     / NULLIF(S_view, 0), 4)     AS CR_view_to_cart,
  ROUND(S_purchase / NULLIF(S_cart, 0), 4)     AS CR_cart_to_purchase,
  ROUND(S_purchase / NULLIF(S_view, 0), 4)     AS CR_view_to_purchase,
  ROUND(1 - S_cart     / NULLIF(S_view, 0), 4) AS DropOff_view_to_cart,
  ROUND(1 - S_purchase / NULLIF(S_cart, 0), 4) AS CartAbandonRate,
  sessions_skip_stage, n_remove
FROM funnel_counts
ORDER BY session_date;

-- 5.2 Tổng thể (mẫu số bằng 0 thì tỷ lệ là NULL, không thay bằng 0)
SELECT S_view, S_cart, S_purchase,
  ROUND(S_cart     / NULLIF(S_view, 0), 4)     AS CR_view_to_cart,
  ROUND(S_purchase / NULLIF(S_cart, 0), 4)     AS CR_cart_to_purchase,
  ROUND(S_purchase / NULLIF(S_view, 0), 4)     AS CR_view_to_purchase,
  ROUND(1 - S_cart     / NULLIF(S_view, 0), 4) AS DropOff_view_to_cart,
  ROUND(1 - S_purchase / NULLIF(S_cart, 0), 4) AS CartAbandonRate,
  sessions_skip_stage, n_remove
FROM (
  SELECT SUM(S_view) AS S_view, SUM(S_cart) AS S_cart, SUM(S_purchase) AS S_purchase,
         SUM(sessions_skip_stage) AS sessions_skip_stage, SUM(n_remove) AS n_remove
  FROM funnel_counts
) t;

-- 5.3 Funnel không thứ tự (đối chiếu với 5.2)
SELECT
  SUM(CAST(has_view     AS INT)) AS sessions_view_any,
  SUM(CAST(has_cart     AS INT)) AS sessions_cart_any,
  SUM(CAST(has_purchase AS INT)) AS sessions_purchase_any
FROM session_funnel;

-- 5.4 Phân rã các phiên không đáp ứng thứ tự (mục 7 và mục 12)
SELECT
  SUM(CAST(sessions_skip_stage AS INT))                                  AS sessions_skip_stage,
  SUM(CASE WHEN has_cart     AND NOT has_cart_ordered     THEN 1 ELSE 0 END) AS cart_without_valid_view,
  SUM(CASE WHEN has_purchase AND NOT has_purchase_ordered THEN 1 ELSE 0 END) AS purchase_without_valid_cart,
  SUM(CASE WHEN has_purchase AND NOT has_cart                THEN 1 ELSE 0 END) AS purchase_without_any_cart,
  SUM(CASE WHEN NOT has_view THEN 1 ELSE 0 END)                          AS sessions_without_view
FROM session_funnel;

-- ---------------------------------------------------------------------
-- 6. SESSION
-- 6.1 Số sự kiện mỗi phiên
-- ---------------------------------------------------------------------
SELECT
  COUNT(*)                                 AS sessions,
  ROUND(AVG(n_events), 2)                  AS avg_events,
  PERCENTILE_APPROX(n_events, 0.5)         AS median_events,
  PERCENTILE_APPROX(n_events, 0.9)         AS p90_events,
  PERCENTILE_APPROX(n_events, 0.99)        AS p99_events,
  MAX(n_events)                            AS max_events
FROM session_funnel;

SELECT g.bucket, g.sessions, ROUND(g.sessions / tot.total, 4) AS share, g.min_events
FROM (
  SELECT bucket, COUNT(*) AS sessions, MIN(n_events) AS min_events
  FROM (
    SELECT n_events,
      CASE WHEN n_events = 1   THEN '1 (bounce)'
           WHEN n_events <= 3  THEN '2-3'
           WHEN n_events <= 10 THEN '4-10'
           ELSE '11+' END AS bucket
    FROM session_funnel
  ) a
  GROUP BY bucket
) g
CROSS JOIN (SELECT COUNT(*) AS total FROM session_funnel) tot
ORDER BY g.min_events;

-- 6.2 Thời lượng phiên (giây) = thời điểm sự kiện cuối - sự kiện đầu
SELECT
  ROUND(AVG(d), 1)          AS avg_sec,
  PERCENTILE_APPROX(d, 0.5) AS median_sec,
  PERCENTILE_APPROX(d, 0.9) AS p90_sec,
  MAX(d)                    AS max_sec,
  SUM(CASE WHEN d = 0 THEN 1 ELSE 0 END) AS sessions_zero_duration
FROM (
  SELECT CAST(session_end_time AS BIGINT) - CAST(session_start_time AS BIGINT) AS d
  FROM session_funnel
) t;

SELECT bucket, sessions, min_d
FROM (
  SELECT bucket, COUNT(*) AS sessions, MIN(d) AS min_d
  FROM (
    SELECT d,
      CASE WHEN d = 0     THEN '0 giây'
           WHEN d <= 60   THEN '1-60 giây'
           WHEN d <= 300  THEN '61-300 giây'
           WHEN d <= 1800 THEN '301-1800 giây'
           ELSE '> 1800 giây' END AS bucket
    FROM (
      SELECT CAST(session_end_time AS BIGINT) - CAST(session_start_time AS BIGINT) AS d
      FROM session_funnel
    ) t
  ) a
  GROUP BY bucket
) g
ORDER BY min_d;

-- 6.3 So sánh phiên có Purchase hợp lệ và phiên không có
SELECT has_purchase_ordered,
  COUNT(*)                AS sessions,
  ROUND(AVG(n_events), 2) AS avg_events,
  ROUND(AVG(CAST(session_end_time AS BIGINT) - CAST(session_start_time AS BIGINT)), 1) AS avg_duration_sec
FROM session_funnel
GROUP BY has_purchase_ordered
ORDER BY has_purchase_ordered;

-- ---------------------------------------------------------------------
-- 7. BRAND (mục 10 của đặc tả)
--    Nhóm CHƯA chốt dùng Phương pháp A hay B cho báo cáo chính.
--    Không so sánh trực tiếp hai kết quả mà không nêu rõ định nghĩa.
--    Một phiên có thể tương tác nhiều brand, nên tổng S_view theo brand
--    lớn hơn số phiên duy nhất. Muốn phân tích theo category: thay brand bằng category.
--    Cột revenue là cột bổ sung (đặc tả chưa định nghĩa), tính từ mọi purchase.
-- 7.1 PHƯƠNG PHÁP A: đơn vị (user_session, brand), cùng brand và đúng thứ tự
-- ---------------------------------------------------------------------
WITH e1 AS (
  SELECT *, MIN(CASE WHEN event_type = 'view' THEN event_time END)
              OVER (PARTITION BY user_session, brand) AS fv
  FROM events_clean
),
e2 AS (
  SELECT *, MIN(CASE WHEN event_type = 'cart' AND event_time >= fv THEN event_time END)
              OVER (PARTITION BY user_session, brand) AS fc
  FROM e1
),
e3 AS (
  SELECT *, MIN(CASE WHEN event_type = 'purchase' AND event_time >= fc THEN event_time END)
              OVER (PARTITION BY user_session, brand) AS fp
  FROM e2
),
sb AS (
  SELECT user_session, brand, MIN(fv) AS fv, MIN(fc) AS fc, MIN(fp) AS fp,
         SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue
  FROM e3
  GROUP BY user_session, brand
),
cnt AS (
  SELECT brand,
    SUM(CAST(fv IS NOT NULL AS INT)) AS S_view,
    SUM(CAST(fc IS NOT NULL AS INT)) AS S_cart,
    SUM(CAST(fp IS NOT NULL AS INT)) AS S_purchase,
    ROUND(SUM(revenue), 2)           AS revenue
  FROM sb
  GROUP BY brand
)
SELECT brand, S_view, S_cart, S_purchase,
  ROUND(S_cart     / NULLIF(S_view, 0), 4) AS CR_view_to_cart,
  ROUND(S_purchase / NULLIF(S_cart, 0), 4) AS CR_cart_to_purchase,
  ROUND(S_purchase / NULLIF(S_view, 0), 4) AS CR_view_to_purchase,
  revenue
FROM cnt
ORDER BY S_view DESC
LIMIT 15;

-- ---------------------------------------------------------------------
-- 7.2 PHƯƠNG PHÁP B: funnel cấp phiên trước (session_funnel), sau đó gán phiên
--     vào mọi brand mà phiên có tương tác. Không đại diện cho conversion riêng
--     của từng brand.
-- ---------------------------------------------------------------------
WITH sb AS (
  SELECT DISTINCT user_session, brand FROM events_clean
),
cnt AS (
  SELECT b.brand,
    SUM(CAST(f.has_view_ordered     AS INT)) AS S_view,
    SUM(CAST(f.has_cart_ordered     AS INT)) AS S_cart,
    SUM(CAST(f.has_purchase_ordered AS INT)) AS S_purchase
  FROM sb b
  JOIN session_funnel f ON b.user_session = f.user_session
  GROUP BY b.brand
)
SELECT brand, S_view, S_cart, S_purchase,
  ROUND(S_cart     / NULLIF(S_view, 0), 4) AS CR_view_to_cart,
  ROUND(S_purchase / NULLIF(S_cart, 0), 4) AS CR_cart_to_purchase,
  ROUND(S_purchase / NULLIF(S_view, 0), 4) AS CR_view_to_purchase
FROM cnt
ORDER BY S_view DESC
LIMIT 15;

-- 7.3 Số phiên tương tác với nhiều brand (giải thích vì sao tổng theo brand vượt tổng phiên)
SELECT
  COUNT(*)                                       AS sessions,
  SUM(CASE WHEN n_brand > 1 THEN 1 ELSE 0 END)   AS sessions_multi_brand
FROM (
  SELECT user_session, COUNT(DISTINCT brand) AS n_brand
  FROM events_clean GROUP BY user_session
) t;

-- ---------------------------------------------------------------------
-- 8. KIỂM TRA TÍNH NHẤT QUÁN (mục 9) - các cột phải là true
-- ---------------------------------------------------------------------
SELECT
  SUM(CAST(has_view_ordered AS INT)) >= SUM(CAST(has_cart_ordered AS INT))     AS ok_view_ge_cart,
  SUM(CAST(has_cart_ordered AS INT)) >= SUM(CAST(has_purchase_ordered AS INT)) AS ok_cart_ge_purchase,
  COUNT(*) = COUNT(DISTINCT user_session)                                      AS ok_one_row_per_session,
  SUM(CASE WHEN (has_purchase_ordered AND NOT has_cart_ordered)
             OR (has_cart_ordered AND NOT has_view_ordered) THEN 1 ELSE 0 END) = 0 AS ok_flag_dependency,
  SUM(CASE WHEN user_session IS NULL THEN 1 ELSE 0 END) = 0                    AS ok_no_null_session,
  CASE WHEN SUM(CAST(has_view_ordered AS INT)) > 0 AND SUM(CAST(has_cart_ordered AS INT)) > 0
       THEN ABS( (SUM(CAST(has_cart_ordered AS INT)) / SUM(CAST(has_view_ordered AS INT)))
               * (SUM(CAST(has_purchase_ordered AS INT)) / SUM(CAST(has_cart_ordered AS INT)))
               - (SUM(CAST(has_purchase_ordered AS INT)) / SUM(CAST(has_view_ordered AS INT))) ) < 1e-9
  END AS ok_product_rule
FROM session_funnel;

-- Đối chiếu số phiên giữa dữ liệu gốc, sau làm sạch và bảng funnel
SELECT
  (SELECT COUNT(DISTINCT user_session) FROM events_parsed)                  AS sessions_raw,
  (SELECT COUNT(DISTINCT user_session) FROM events_clean)                   AS sessions_clean,
  (SELECT COUNT(*) FROM session_funnel)                                     AS sessions_funnel,
  (SELECT SUM(S_view) FROM funnel_counts)                                   AS S_view_total,
  (SELECT COUNT(*) FROM session_funnel WHERE NOT has_view)                  AS sessions_without_view,
  (SELECT COUNT(*) FROM events_parsed) - (SELECT COUNT(*) FROM events_clean) AS rows_removed;

-- ---------------------------------------------------------------------
-- 9. KẾT QUẢ THAM CHIẾU (tính bằng SQLite với cùng logic, trên sample_20k.csv)
-- 2.1  total_rows=20000, null_session=0, null_event_type=0, null_product_id=0,
--      null_brand=3401, null_category=7110, bad_timestamp=0, price_le_0=8,
--      sessions_raw=5231, thời gian từ 2019-11-01 00:00:00 đến 01:47:47
-- 2.2  view=19671, purchase=183, cart=146, không có remove_from_cart
-- 2.3  dup_groups=5, dup_extra_rows=5
-- 5.1  chỉ có 1 ngày: 2019-11-01
-- 5.2  S_view=5229, S_cart=97, S_purchase=49, sessions_skip_stage=106, n_remove=0
--      CR_view_to_cart=0.0186, CR_cart_to_purchase=0.5052, CR_view_to_purchase=0.0094
--      DropOff_view_to_cart=0.9814, CartAbandonRate=0.4948
-- 5.3  view=5229, cart=99, purchase=153
-- 5.4  skip=106, cart_without_valid_view=2, purchase_without_valid_cart=104,
--      sessions_without_view=2
-- 6.1  sessions=5231, avg_events=3.82, median=2, p90 khoảng 9, p99 khoảng 25, max=90
--      bucket: 1=2152, 2-3=1409, 4-10=1284, 11+=386
-- 6.2  avg_sec=176.7, median=41, max=5594, zero_duration=2154
--      bucket: 0=2154, 1-60=721, 61-300=1428, 301-1800=882, >1800=46
--      (PERCENTILE_APPROX là xấp xỉ nên có thể lệch nhẹ so với giá trị chính xác)
-- 6.3  has_purchase_ordered=true : 49 phiên, avg_events=8.67, avg_duration=419.3
--      has_purchase_ordered=false: 5182 phiên, avg_events=3.78, avg_duration=174.4
-- 7.1  Phương pháp A (S_view, S_cart, S_purchase, revenue):
--      unknown 1188/1/0 (3502.74), samsung 829/26/12 (11795.17),
--      apple 668/36/22 (31833.65), xiaomi 467/16/7 (1270.06), huawei 173/3/1 (228.55)
-- 7.2  Phương pháp B (S_view, S_cart, S_purchase):
--      unknown 1188/6/4, samsung 829/38/20, apple 668/39/24, xiaomi 467/18/7, huawei 173/8/3
-- 7.3  sessions=5231, sessions_multi_brand=1667 (tổng S_view theo brand = 8832 > 5229)
-- 8    sessions_raw=sessions_clean=sessions_funnel=5231, S_view_total=5229,
--      sessions_without_view=2, rows_removed=0
-- ---------------------------------------------------------------------
