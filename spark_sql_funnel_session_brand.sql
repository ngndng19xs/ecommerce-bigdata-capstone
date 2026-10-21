-- =====================================================================
-- TRUY VẤN SPARK SQL MẪU: FUNNEL / SESSION / BRAND   (phiên bản 2)
-- Bám theo: "Đặc tả kỹ thuật: Thống nhất định nghĩa funnel TMĐT" (Tuần 1 - T004)
-- Vai trò : TV2 (Big Data Analytics)
-- Quy ước : UTC | đếm theo user_session | funnel TUẦN TỰ là chính thức
--           so sánh thời gian dùng >= | brand/category null -> 'unknown'
-- Lưu ý   : file này CHƯA được chạy thử (môi trường người soạn không có Spark).
--           Chạy trên Spark và đối chiếu với "MONG ĐỢI" (tính tay).
-- =====================================================================

SET spark.sql.session.timeZone = UTC;

-- ---------------------------------------------------------------------
-- 1. DỮ LIỆU MẪU: 9 phiên + 1 dòng thiếu user_session
--  S1 chỉ xem | S2 xem->giỏ->bỏ khỏi giỏ | S3 xem->giỏ->mua
--  S4 xem->giỏ->mua, qua nửa đêm | S5 chỉ purchase, brand null
--  S6 xem->giỏ | S7 giỏ(trước view)->xem->giỏ->mua  (mục 4.2: vẫn hợp lệ)
--  S8 xem->mua->giỏ (purchase trước cart: không hợp lệ)
--  S9 nhiều brand: xem apple, giỏ+mua samsung (để so Phương pháp A và B)
--  NULL: dòng thiếu user_session (phải bị loại và thống kê, mục 2 và 12)
-- Dữ liệu thật: BỎ mục 1, dùng bảng sự kiện chuẩn hóa của TV1.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW events_sample AS
SELECT * FROM VALUES
  (TIMESTAMP '2019-10-01 10:00:00','view',            101,'apple',   900.0, 1,'S1'),
  (TIMESTAMP '2019-10-01 10:01:00','view',            102,'apple',   950.0, 1,'S1'),
  (TIMESTAMP '2019-10-01 11:00:00','view',            101,'apple',   900.0, 2,'S2'),
  (TIMESTAMP '2019-10-01 11:02:00','cart',            101,'apple',   900.0, 2,'S2'),
  (TIMESTAMP '2019-10-01 11:05:00','remove_from_cart',101,'apple',   900.0, 2,'S2'),
  (TIMESTAMP '2019-10-01 12:00:00','view',            201,'samsung', 300.0, 3,'S3'),
  (TIMESTAMP '2019-10-01 12:03:00','cart',            201,'samsung', 300.0, 3,'S3'),
  (TIMESTAMP '2019-10-01 12:10:00','purchase',        201,'samsung', 300.0, 3,'S3'),
  (TIMESTAMP '2019-10-01 23:50:00','view',            103,'apple',  1000.0, 4,'S4'),
  (TIMESTAMP '2019-10-01 23:58:00','cart',            103,'apple',  1000.0, 4,'S4'),
  (TIMESTAMP '2019-10-02 00:05:00','purchase',        103,'apple',  1000.0, 4,'S4'),
  (TIMESTAMP '2019-10-02 13:00:00','purchase',        301, NULL,      50.0, 5,'S5'),
  (TIMESTAMP '2019-10-02 14:00:00','view',            401,'xiaomi',  200.0, 6,'S6'),
  (TIMESTAMP '2019-10-02 14:01:00','cart',            401,'xiaomi',  200.0, 6,'S6'),
  (TIMESTAMP '2019-10-02 15:00:00','cart',            202,'samsung', 400.0, 7,'S7'),
  (TIMESTAMP '2019-10-02 15:01:00','view',            202,'samsung', 400.0, 7,'S7'),
  (TIMESTAMP '2019-10-02 15:02:00','cart',            202,'samsung', 400.0, 7,'S7'),
  (TIMESTAMP '2019-10-02 15:03:00','purchase',        202,'samsung', 400.0, 7,'S7'),
  (TIMESTAMP '2019-10-02 16:00:00','view',            402,'xiaomi',  150.0, 8,'S8'),
  (TIMESTAMP '2019-10-02 16:01:00','purchase',        402,'xiaomi',  150.0, 8,'S8'),
  (TIMESTAMP '2019-10-02 16:02:00','cart',            402,'xiaomi',  150.0, 8,'S8'),
  (TIMESTAMP '2019-10-02 17:00:00','view',            104,'apple',   800.0, 9,'S9'),
  (TIMESTAMP '2019-10-02 17:01:00','cart',            203,'samsung', 350.0, 9,'S9'),
  (TIMESTAMP '2019-10-02 17:02:00','purchase',        203,'samsung', 350.0, 9,'S9'),
  (TIMESTAMP '2019-10-03 09:00:00','view',            105,'apple',   100.0,10, NULL)
AS t(event_time, event_type, product_id, brand, price, user_id, user_session);

-- ---------------------------------------------------------------------
-- 2. KIỂM TRA CHẤT LƯỢNG TRƯỚC KHI TÍNH (mục 12 của đặc tả)
-- ---------------------------------------------------------------------
SELECT
  COUNT(*)                                                        AS total_rows,
  SUM(CASE WHEN user_session IS NULL THEN 1 ELSE 0 END)           AS rows_null_session,
  SUM(CASE WHEN event_time   IS NULL THEN 1 ELSE 0 END)           AS rows_null_time,
  SUM(CASE WHEN brand        IS NULL THEN 1 ELSE 0 END)           AS rows_null_brand,
  COUNT(DISTINCT user_session)                                    AS sessions_raw
FROM events_sample;
-- MONG ĐỢI: 25 | 1 | 0 | 1 | 9

SELECT event_type, COUNT(*) AS n FROM events_sample GROUP BY event_type ORDER BY event_type;
-- MONG ĐỢI: cart=8, purchase=6, remove_from_cart=1, view=10

-- Bảng sự kiện chuẩn hóa (mục 11.1): loại session null, brand null -> 'unknown',
-- thêm session_date = DATE(MIN(event_time)) theo phiên.
CREATE OR REPLACE TEMP VIEW events_clean AS
SELECT event_time,
       DATE(event_time)                                       AS event_date,
       event_type, product_id,
       COALESCE(brand,'unknown')                              AS brand,
       price, user_id, user_session,
       DATE(MIN(event_time) OVER (PARTITION BY user_session)) AS session_date
FROM events_sample
WHERE user_session IS NOT NULL;

-- ---------------------------------------------------------------------
-- 3. BẢNG FUNNEL CẤP PHIÊN (mục 11.2) - mỗi phiên 1 dòng
--  fv = first_view_time
--  fc = first_cart_after_view_time     : MIN(cart có event_time >= fv)   (mục 4.2)
--  fp = first_purchase_after_cart_time : MIN(purchase có event_time >= fc) (mục 4.3)
--  Điều kiện tính TRONG phần MIN (không lấy cart đầu tiên rồi mới so với view),
--  để cart nằm trước view không làm mất cart hợp lệ đến sau.
--  Cờ *_ordered = funnel tuần tự | has_view/has_cart/has_purchase = không thứ tự.
--  is_skip_stage (mục 7): có cart/purchase nhưng không đạt điều kiện thứ tự.
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW session_funnel AS
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
    FIRST(user_id)  AS user_id,
    MIN(session_date) AS session_date,
    MIN(event_time) AS session_start_time,
    MAX(event_time) AS session_end_time,
    MIN(fv) AS first_view_time,
    MIN(fc) AS first_cart_after_view_time,
    MIN(fp) AS first_purchase_after_cart_time,
    COUNT(*) AS n_events,
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
   (has_purchase AND NOT has_purchase_ordered))   AS is_skip_stage
FROM flags;

SELECT user_session, session_date,
       has_view_ordered, has_cart_ordered, has_purchase_ordered,
       has_view, has_cart, has_purchase, is_skip_stage
FROM session_funnel ORDER BY user_session;
-- MONG ĐỢI (ordered V/C/P | unordered V/C/P | skip):
-- S1 T/F/F | T/F/F | F      S2 T/T/F | T/T/F | F
-- S3 T/T/T | T/T/T | F      S4 T/T/T | T/T/T | F
-- S5 F/F/F | F/F/T | T      S6 T/T/F | T/T/F | F
-- S7 T/T/T | T/T/T | F  (cart trước view nhưng có cart hợp lệ sau view)
-- S8 T/T/F | T/T/T | T  (purchase trước cart)
-- S9 T/T/T | T/T/T | F

-- ---------------------------------------------------------------------
-- 4. FUNNEL
-- 4.1 Bảng tổng hợp theo ngày (mục 11.3) - dùng session_date
-- ---------------------------------------------------------------------
CREATE OR REPLACE TEMP VIEW funnel_counts AS
SELECT session_date,
  SUM(CAST(has_view_ordered     AS INT)) AS S_view,
  SUM(CAST(has_cart_ordered     AS INT)) AS S_cart,
  SUM(CAST(has_purchase_ordered AS INT)) AS S_purchase,
  SUM(CAST(is_skip_stage        AS INT)) AS sessions_skip_stage,
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
-- MONG ĐỢI:
-- 2019-10-01: S_view=4, S_cart=3, S_purchase=2, v2c=0.75, c2p=0.6667, v2p=0.5, skip=0, n_remove=1
-- 2019-10-02: S_view=4, S_cart=4, S_purchase=2, v2c=1.0, c2p=0.5, v2p=0.5, skip=2, n_remove=0
-- (mẫu số = 0 thì tỷ lệ = NULL, KHÔNG thay bằng 0 - mục 8.6)

-- 4.2 Tổng thể
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
-- MONG ĐỢI: S_view=8, S_cart=7, S_purchase=4 | CR: 0.875, 0.5714, 0.5
--           DropOff=0.125, CartAbandonRate=0.4286 | skip=2 (S5, S8), n_remove=1
-- Kiểm tra: 0.875 x 0.5714 = 0.5

-- 4.3 Funnel KHÔNG thứ tự (đối chiếu, mục 5.2)
SELECT
  SUM(CAST(has_view     AS INT)) AS sessions_view_any,
  SUM(CAST(has_cart     AS INT)) AS sessions_cart_any,
  SUM(CAST(has_purchase AS INT)) AS sessions_purchase_any
FROM session_funnel;
-- MONG ĐỢI: 8 / 7 / 6  (purchase 6 > 4 vì có S5, S8 không đúng thứ tự)

-- 4.4 Danh sách phiên không đáp ứng thứ tự (để kiểm tra bằng mắt)
SELECT user_session, has_view, has_cart, has_purchase,
       has_cart_ordered, has_purchase_ordered
FROM session_funnel WHERE is_skip_stage ORDER BY user_session;
-- MONG ĐỢI: S5, S8

-- ---------------------------------------------------------------------
-- 5. SESSION
-- 5.1 Số sự kiện mỗi phiên
-- ---------------------------------------------------------------------
SELECT user_session, n_events, n_view, n_cart, n_remove, n_purchase
FROM session_funnel ORDER BY user_session;
-- MONG ĐỢI n_events: S1=2 S2=3 S3=3 S4=3 S5=1 S6=2 S7=4 S8=3 S9=3

SELECT ROUND(AVG(n_events), 2)          AS avg_events,
       PERCENTILE_APPROX(n_events, 0.5) AS median_events,
       MAX(n_events)                    AS max_events
FROM session_funnel;
-- MONG ĐỢI: avg=2.67, median=3, max=4

SELECT CASE WHEN n_events = 1 THEN '1 (bounce)'
            WHEN n_events BETWEEN 2 AND 3 THEN '2-3'
            ELSE '4+' END AS bucket,
       COUNT(*) AS sessions
FROM session_funnel GROUP BY 1 ORDER BY 1;
-- MONG ĐỢI: '1 (bounce)'=1, '2-3'=7, '4+'=1

-- 5.2 Thời lượng phiên (giây)
SELECT user_session,
  UNIX_TIMESTAMP(session_end_time) - UNIX_TIMESTAMP(session_start_time) AS duration_sec
FROM session_funnel ORDER BY user_session;
-- MONG ĐỢI: S1=60 S2=300 S3=600 S4=900 S5=0 S6=60 S7=180 S8=120 S9=120

SELECT ROUND(AVG(d), 1) AS avg_sec, PERCENTILE_APPROX(d, 0.5) AS median_sec, MAX(d) AS max_sec
FROM (SELECT UNIX_TIMESTAMP(session_end_time) - UNIX_TIMESTAMP(session_start_time) AS d
      FROM session_funnel) t;
-- MONG ĐỢI: avg=260.0, median=120, max=900

-- 5.3 Phiên có mua (hợp lệ) và không mua
SELECT has_purchase_ordered,
  COUNT(*) AS sessions,
  ROUND(AVG(n_events), 2) AS avg_events,
  ROUND(AVG(UNIX_TIMESTAMP(session_end_time) - UNIX_TIMESTAMP(session_start_time)), 1) AS avg_duration_sec
FROM session_funnel GROUP BY has_purchase_ordered;
-- MONG ĐỢI: true  -> 4 phiên, 3.25 sự kiện, 450.0 giây
--           false -> 5 phiên, 2.2 sự kiện, 108.0 giây

-- ---------------------------------------------------------------------
-- 6. BRAND (mục 10 của đặc tả: nhóm CHƯA chốt Phương pháp A hay B).
--    Không trộn A và B trong cùng một chỉ số / báo cáo.
-- 6.1 PHƯƠNG PHÁP A: đơn vị (user_session, brand), cart/purchase phải CÙNG brand
--     và đúng thứ tự View -> Cart -> Purchase.
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
  FROM e3 GROUP BY user_session, brand
),
cnt AS (
  SELECT brand,
    SUM(CAST(fv IS NOT NULL AS INT)) AS S_view,
    SUM(CAST(fc IS NOT NULL AS INT)) AS S_cart,
    SUM(CAST(fp IS NOT NULL AS INT)) AS S_purchase,
    SUM(revenue) AS revenue
  FROM sb GROUP BY brand
)
SELECT brand, S_view, S_cart, S_purchase,
  ROUND(S_cart     / NULLIF(S_view, 0), 4) AS CR_view_to_cart,
  ROUND(S_purchase / NULLIF(S_cart, 0), 4) AS CR_cart_to_purchase,
  ROUND(S_purchase / NULLIF(S_view, 0), 4) AS CR_view_to_purchase,
  revenue
FROM cnt ORDER BY revenue DESC;
-- MONG ĐỢI (Phương pháp A):
-- samsung: 2, 2, 2 | 1.0, 1.0, 1.0   | revenue 1050
-- apple  : 4, 2, 1 | 0.5, 0.5, 0.25  | revenue 1000
-- xiaomi : 2, 2, 0 | 1.0, 0.0, 0.0   | revenue 150
-- unknown: 0, 0, 0 | NULL, NULL, NULL| revenue 50
-- S9-samsung không có view samsung nên KHÔNG vào funnel samsung (khác Phương pháp B).
-- revenue = tổng mọi purchase của brand (gồm cả phiên S5, S8 không đúng thứ tự).
-- CẦN CHỐT: revenue tính từ mọi purchase hay chỉ từ purchase hợp lệ.

-- ---------------------------------------------------------------------
-- 6.2 PHƯƠNG PHÁP B: tính funnel cấp phiên trước (session_funnel),
--     sau đó gán phiên vào mọi brand mà phiên có tương tác.
--     Không phản ánh conversion riêng của brand, chỉ là hành vi của nhóm phiên.
-- ---------------------------------------------------------------------
WITH sb AS (SELECT DISTINCT user_session, brand FROM events_clean),
cnt AS (
  SELECT b.brand,
    SUM(CAST(f.has_view_ordered     AS INT)) AS S_view,
    SUM(CAST(f.has_cart_ordered     AS INT)) AS S_cart,
    SUM(CAST(f.has_purchase_ordered AS INT)) AS S_purchase
  FROM sb b JOIN session_funnel f ON b.user_session = f.user_session
  GROUP BY b.brand
)
SELECT brand, S_view, S_cart, S_purchase,
  ROUND(S_cart     / NULLIF(S_view, 0), 4) AS CR_view_to_cart,
  ROUND(S_purchase / NULLIF(S_cart, 0), 4) AS CR_cart_to_purchase,
  ROUND(S_purchase / NULLIF(S_view, 0), 4) AS CR_view_to_purchase
FROM cnt ORDER BY S_view DESC;
-- MONG ĐỢI (Phương pháp B):
-- apple  : 4, 3, 2 | 0.75, 0.6667, 0.5
-- samsung: 3, 3, 3 | 1.0, 1.0, 1.0
-- xiaomi : 2, 2, 0 | 1.0, 0.0, 0.0
-- unknown: 0, 0, 0 | NULL, NULL, NULL
-- Tổng S_view theo brand = 9 > 8 phiên duy nhất, vì S9 được đếm ở cả apple và samsung.
-- So sánh với 6.1: apple và samsung khác nhau do S9 -> cần thống nhất trước khi báo cáo.

-- ---------------------------------------------------------------------
-- 7. KIỂM TRA TÍNH NHẤT QUÁN (mục 9 của đặc tả) - các cột phải = true
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

-- Đối chiếu số phiên: dữ liệu gốc = sau làm sạch = bảng funnel
SELECT
  (SELECT COUNT(DISTINCT user_session) FROM events_sample) AS sessions_raw,
  (SELECT COUNT(DISTINCT user_session) FROM events_clean)  AS sessions_clean,
  (SELECT COUNT(*) FROM session_funnel)                    AS sessions_funnel;
-- MONG ĐỢI: 9 | 9 | 9

-- ---------------------------------------------------------------------
-- 8. CHUYỂN SANG DỮ LIỆU THẬT
--  - Bỏ mục 1, thay events_sample/events_clean bằng bảng Hive/Parquet của TV1
--  - session_funnel nên ghi ra bảng Parquet (CREATE TABLE ... USING parquet AS)
--    vì 3 lớp window trên 100M+ dòng khá nặng, không nên tính lại nhiều lần.
--  - Chạy thử 1-2 ngày: lọc theo session_date SAU khi đã có session_funnel.
--    Lọc event_date trước có thể cắt mất sự kiện cuối của phiên qua nửa đêm.
-- ---------------------------------------------------------------------
