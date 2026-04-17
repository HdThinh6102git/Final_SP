/*
 * SP_RPA_KBG
 * Description : Process KB insurance data
 * Parameters  :
 *   IN_BATCH_ID       : Batch ID to process
 *   IN_INSURANCE_TYPE : Insurance type (LTR / CAR / GEN)
 *   IN_CONTRACT_TYPE  : Contract type (NEW only)
 * Steps       :
 *   1. Hardcoded column mapping by insurance type (LTR / CAR / GEN)
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules (LTR / CAR / GEN)
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_KBG`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'KBG';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT '';
    DECLARE v_processed_table VARCHAR(100) DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_keep_row;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_agg_data;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_all_normal;
    END;

    -- 1. Hardcoded Column Mapping
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';

        -- Mapping for LTR contracts (Columns 01-28 + Target-only 29-31)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 대리점코드
            'COLUMN_02, ', -- 지사코드
            'COLUMN_03, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 대리점코드
            'COLUMN_02, ', -- 지사코드
            'COLUMN_03, '); -- 지사명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 지점코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 지점코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 회계일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 회계일

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 상품명
            'COLUMN_11, ', -- 상품코드
            'COLUMN_12, '); -- 보험시기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 상품명
            'COLUMN_11, ', -- 상품코드
            'COLUMN_12, '); -- 보험시기

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 태아여부
            'COLUMN_14, ', -- 태아선지급
            'COLUMN_15, '); -- 계약자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 태아여부
            'COLUMN_14, ', -- 태아선지급
            'COLUMN_15, '); -- 계약자

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 계약자주민번호
            'COLUMN_17, ', -- 피보험자
            'COLUMN_18, '); -- 피보험자주민번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 계약자주민번호
            'COLUMN_17, ', -- 피보험자
            'COLUMN_18, '); -- 피보험자주민번호

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 건수
            'COLUMN_20, ', -- 보험료
            'COLUMN_21, '); -- 보장보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 건수
            'COLUMN_20, ', -- 보험료
            'COLUMN_21, '); -- 보장보험료

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 수정보험료
            'COLUMN_23, ', -- 납방
            'COLUMN_24, '); -- 납기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 수정보험료
            'COLUMN_23, ', -- 납방
            'COLUMN_24, '); -- 납기

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 보기
            'COLUMN_26, ', -- 군
            'COLUMN_27, '); -- 구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 보기
            'COLUMN_26, ', -- 군
            'COLUMN_27, '); -- 구분

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 매출발생일자
            'NULL, ',      -- 납기구분
            'NULL, ');     -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 매출발생일자
            'COLUMN_29, ', -- 납기구분
            'COLUMN_30, '); -- 납입월

        -- 31
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL');       -- 납입일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31');  -- 납입일

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        SET v_raw_table = 'T_RPA_CAR_RAW';
        SET v_processed_table = 'T_RPA_CAR_PROCESSED';

        -- Mapping for CAR contracts (Columns 01-25 + Target-only 26-28)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 대리점코드
            'COLUMN_02, ', -- 지사코드
            'COLUMN_03, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 대리점코드
            'COLUMN_02, ', -- 지사코드
            'COLUMN_03, '); -- 지사명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 지점코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 지점코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 등급
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 등급

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 회계일
            'COLUMN_11, ', -- 상품코드
            'COLUMN_12, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 회계일
            'COLUMN_11, ', -- 상품코드
            'COLUMN_12, '); -- 상품명

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 계약자명
            'COLUMN_14, ', -- 계약자 주민번호
            'COLUMN_15, '); -- 피보험자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 계약자명
            'COLUMN_14, ', -- 계약자 주민번호
            'COLUMN_15, '); -- 피보험자명

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 피보험자 주민번호
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 건수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 피보험자 주민번호
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 건수

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보험료
            'COLUMN_20, ', -- 인수유형
            'COLUMN_21, '); -- 구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보험료
            'COLUMN_20, ', -- 인수유형
            'COLUMN_21, '); -- 구분

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 스코어
            'COLUMN_23, ', -- 매출발생일자
            'COLUMN_24, '); -- 보험시작일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 스코어
            'COLUMN_23, ', -- 매출발생일자
            'COLUMN_24, '); -- 보험시작일자

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 보험종료일자
            'NULL, ',      -- 납기구분
            'NULL, ');     -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 보험종료일자
            'COLUMN_26, ', -- 납기구분
            'COLUMN_27, '); -- 납입월

        -- 28
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL');       -- 납입주기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28');  -- 납입주기

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        SET v_raw_table = 'T_RPA_GENERAL_RAW';
        SET v_processed_table = 'T_RPA_GENERAL_PROCESSED';

        -- Mapping for GEN contracts (Columns 01-20 + Target-only 21-23)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 대리점코드
            'COLUMN_02, ', -- 지사코드
            'COLUMN_03, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 대리점코드
            'COLUMN_02, ', -- 지사코드
            'COLUMN_03, '); -- 지사명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 지점코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 지점코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 회계일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 지점명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 회계일

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 상품코드
            'COLUMN_11, ', -- 상품명
            'COLUMN_12, '); -- 계약자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 상품코드
            'COLUMN_11, ', -- 상품명
            'COLUMN_12, '); -- 계약자명

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 주민번호
            'COLUMN_14, ', -- 건수
            'COLUMN_15, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 주민번호
            'COLUMN_14, ', -- 건수
            'COLUMN_15, '); -- 보험료

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 구분
            'COLUMN_17, ', -- 납방
            'COLUMN_18, '); -- 입력일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 구분
            'COLUMN_17, ', -- 납방
            'COLUMN_18, '); -- 입력일

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보험시작일자
            'COLUMN_20, ', -- 보험종료일자
            'NULL, ');     -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보험시작일자
            'COLUMN_20, ', -- 보험종료일자
            'COLUMN_21, '); -- 납기구분

        -- 22-23
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납입월
            'NULL');  -- 납입일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 납입월
            'COLUMN_23');  -- 납입일
    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_KBG_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_KBG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_08 <> ''증권번호'';'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 2.3. Apply transformation rules
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 맨 마지막열 값 추가(3개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납입일 / 항목값 : 회계일과 동일한 값으로 반영
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET COLUMN_29 = '년납',
                COLUMN_30 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_31 = COLUMN_09;

            -- Rule 2: [증권번호] 오름차순 정렬 정렬 후 [보험시기] ≠해당월이면 삭제
            SET @seq := 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_08 ASC, EXCEL_ROW_INDEX ASC;

            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE LEFT(REPLACE(REPLACE(COLUMN_12, '-', ''), '.', ''), 6) <> DATE_FORMAT(CURDATE(), '%Y%m');

            -- Rule 3: 증권번호 중복 편집
            -- ① 중복 증권번호의 [구분]=각각"취소,정상"이면 [구분]="취소"로 수정 및 중복데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_case;
            CREATE TEMPORARY TABLE tmp_kbg_dup_case
            SELECT COLUMN_08
            FROM T_TEMP_RPA_KBG_PROCESSED
            GROUP BY COLUMN_08
            HAVING SUM(CASE WHEN COLUMN_27 = '취소' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_27 = '정상' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_dup_case d ON t.COLUMN_08 = d.COLUMN_08
            SET t.COLUMN_27 = '취소';

            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_keep_row;
            CREATE TEMPORARY TABLE tmp_kbg_keep_row
            SELECT COLUMN_08, MIN(SORT_ORDER_NO) AS keep_sort_order_no
            FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_kbg_dup_case)
            GROUP BY COLUMN_08;

            DELETE t
            FROM T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_keep_row k
                    ON t.COLUMN_08 = k.COLUMN_08
            WHERE t.COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_kbg_dup_case)
              AND t.SORT_ORDER_NO <> k.keep_sort_order_no;

            -- ② 중복 증번번호 중 [상품명]="KB 금쪽같은 자녀보험"이면 [보험료],[수정보험료]를 합산하여 한건으로 값수정([건수]="0"인 데이터 행삭제)
            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_agg_data;
            CREATE TEMPORARY TABLE tmp_kbg_agg_data
            SELECT
                a.COLUMN_08,
                SUM(CAST(REPLACE(IFNULL(a.COLUMN_20, '0'), ',', '') AS DECIMAL(18,0))) AS sum_20,
                SUM(CAST(REPLACE(IFNULL(a.COLUMN_22, '0'), ',', '') AS DECIMAL(18,0))) AS sum_22,
                COALESCE(
                    MIN(CASE WHEN CAST(IFNULL(a.COLUMN_19, '0') AS SIGNED) <> 0 THEN a.SORT_ORDER_NO END),
                    MIN(a.SORT_ORDER_NO)
                ) AS keep_sort_order_no
            FROM T_TEMP_RPA_KBG_PROCESSED a
            WHERE a.COLUMN_10 LIKE '%KB 금쪽같은 자녀보험%'
            GROUP BY a.COLUMN_08
            HAVING COUNT(*) > 1;

            UPDATE T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_agg_data a
                    ON t.COLUMN_08 = a.COLUMN_08
                AND t.SORT_ORDER_NO = a.keep_sort_order_no
            SET t.COLUMN_20 = CAST(a.sum_20 AS CHAR),
                t.COLUMN_22 = CAST(a.sum_22 AS CHAR);

            DELETE t
            FROM T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_agg_data a
                    ON t.COLUMN_08 = a.COLUMN_08
            WHERE t.COLUMN_10 LIKE '%KB 금쪽같은 자녀보험%'
              AND t.SORT_ORDER_NO <> a.keep_sort_order_no;

            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_10 LIKE '%KB 금쪽같은 자녀보험%'
              AND (COLUMN_19 = '0' OR CAST(IFNULL(COLUMN_19, '0') AS SIGNED) = 0);

            -- Rule 4: [보험료],[수정보험료]="마이너스금액"이면 "플러스"로 변경
            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET COLUMN_20 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_20, '0'), ',', '') AS DECIMAL(18,0))) AS CHAR)
            WHERE REPLACE(IFNULL(COLUMN_20, '0'), ',', '') REGEXP '^-[0-9]+';

            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET COLUMN_22 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_22, '0'), ',', '') AS DECIMAL(18,0))) AS CHAR)
            WHERE REPLACE(IFNULL(COLUMN_22, '0'), ',', '') REGEXP '^-[0-9]+';

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 맨 마지막열 값 추가(3개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납입주기 / 항목값 : 일시납
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET COLUMN_26 = '년납',
                COLUMN_27 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_28 = COLUMN_10;

            -- Rule 2: [증권번호] 오름차순 정렬 후 [구분]="환추징"이면 데이터 행삭제
            SET @seq := 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_08 ASC, EXCEL_ROW_INDEX ASC;

            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_21 = '환추징';

            -- Rule 3: [보험시작일]<"해당월"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE LEFT(REPLACE(REPLACE(COLUMN_24, '-', ''), '.', ''), 6) < DATE_FORMAT(CURDATE(), '%Y%m');

            -- Rule 4: 중복 증번 편집
            -- ① 중복 증권번호 중 [구분]="정상,취소"이면 [구분]="취소" 값수정 및 [보험료]="마이너스금액" 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_case;
            CREATE TEMPORARY TABLE tmp_kbg_dup_case
            SELECT COLUMN_08
            FROM T_TEMP_RPA_KBG_PROCESSED
            GROUP BY COLUMN_08
            HAVING SUM(CASE WHEN COLUMN_21 = '취소' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_21 = '정상' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_dup_case d ON t.COLUMN_08 = d.COLUMN_08
            SET t.COLUMN_21 = '취소';

            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_kbg_dup_case)
              AND REPLACE(IFNULL(COLUMN_19, '0'), ',', '') REGEXP '^-[0-9]+';

            -- ② 중복 증권번호 중 [상품명]="공동"이면 [보험료]="보험료 합산" 한건으로 수정 및 중복 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_agg_data;
            CREATE TEMPORARY TABLE tmp_kbg_agg_data
            SELECT
                COLUMN_08,
                SUM(CAST(REPLACE(IFNULL(COLUMN_19, '0'), ',', '') AS DECIMAL(18,0))) AS sum_19,
                MIN(SORT_ORDER_NO) AS keep_sort_order_no
            FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_12 LIKE '%공동%'
            GROUP BY COLUMN_08
            HAVING COUNT(*) > 1;

            UPDATE T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_agg_data a
                    ON t.COLUMN_08 = a.COLUMN_08
                   AND t.SORT_ORDER_NO = a.keep_sort_order_no
            SET t.COLUMN_19 = CAST(a.sum_19 AS CHAR);

            DELETE t
            FROM T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_agg_data a
                    ON t.COLUMN_08 = a.COLUMN_08
            WHERE t.COLUMN_12 LIKE '%공동%'
              AND t.SORT_ORDER_NO <> a.keep_sort_order_no;

            -- ③ 중복 증권번호 중 [구분]="정상"으로 동일한 경우, [건수]="0"인 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_all_normal;
            CREATE TEMPORARY TABLE tmp_kbg_dup_all_normal
            SELECT COLUMN_08
            FROM T_TEMP_RPA_KBG_PROCESSED
            GROUP BY COLUMN_08
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_21 <> '정상' THEN 1 ELSE 0 END) = 0;

            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_kbg_dup_all_normal)
              AND (COLUMN_18 = '0' OR CAST(IFNULL(COLUMN_18, '0') AS SIGNED) = 0);

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 맨 마지막열 값 추가(3개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납입일 / 항목값 : 회계일과 동일한 값으로 반영
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_KBG_PROCESSED
            SET COLUMN_21 = '년납',
                COLUMN_22 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_23 = COLUMN_09;

            -- Rule 2: 중복 증권번호 중 [구분]="정상,취소"이면 [구분]="취소" 값수정 및 [보험료]="마이너스금액" 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_case;
            CREATE TEMPORARY TABLE tmp_kbg_dup_case
            SELECT COLUMN_08
            FROM T_TEMP_RPA_KBG_PROCESSED
            GROUP BY COLUMN_08
            HAVING SUM(CASE WHEN COLUMN_16 = '취소' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_16 = '정상' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN tmp_kbg_dup_case d ON t.COLUMN_08 = d.COLUMN_08
            SET t.COLUMN_16 = '취소';

            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_kbg_dup_case)
              AND REPLACE(IFNULL(COLUMN_15, '0'), ',', '') REGEXP '^-[0-9]+';

            -- Rule 3: [건수]="0"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE COLUMN_14 = '0'
               OR CAST(IFNULL(COLUMN_14, '0') AS SIGNED) = 0;

            -- Rule 4: [건수]="1" & [납입방법]≠"월납,일시납"이고 [보험시작일자]≠"해당월"인 데이터 행삭제
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED
            WHERE (COLUMN_14 = '1' OR CAST(IFNULL(COLUMN_14, '0') AS SIGNED) = 1)
              AND COLUMN_17 NOT IN ('월납', '일시납')
              AND LEFT(REPLACE(REPLACE(COLUMN_19, '-', ''), '.', ''), 6) <> DATE_FORMAT(CURDATE(), '%Y%m');

        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_KBG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_keep_row;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_agg_data;
        DROP TEMPORARY TABLE IF EXISTS tmp_kbg_dup_all_normal;

    END IF;

END