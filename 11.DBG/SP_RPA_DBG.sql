/*
 * SP_RPA_DBG
 * Description : Process DB Non-life insurance data
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

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_DBG`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'DBG';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT '';
    DECLARE v_processed_table VARCHAR(100) DEFAULT '';

    -- [DECLARE debug variables]
    DECLARE v_log_initial_raw  INT DEFAULT 0;
    DECLARE v_log_temp_initial INT DEFAULT 0;
    DECLARE v_log_after_rule2  INT DEFAULT 0;
    DECLARE v_log_after_rule3  INT DEFAULT 0;
    DECLARE v_log_after_rule4  INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @v_err_no = MYSQL_ERRNO,
            @v_err_msg = MESSAGE_TEXT;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (
            IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
            CONCAT('SQL_EXCEPTION: [', @v_err_no, '] ', @v_err_msg),
            0, NOW()
        );
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;
        DROP TEMPORARY TABLE IF EXISTS tmp_mix_cancel;
    END;

    -- [INIT Debug Log Table]
    CREATE TABLE IF NOT EXISTS T_RPA_DEBUG_LOG (
        BATCH_ID       VARCHAR(100),
        COMPANY_CODE   VARCHAR(10),
        INSURANCE_TYPE VARCHAR(50),
        CONTRACT_TYPE  VARCHAR(20),
        STEP_NAME      VARCHAR(100),
        ROW_COUNT      INT,
        LOG_TIME       DATETIME
    );

    -- 1. Hardcoded Column Mapping
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
            SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_table = 'T_RPA_CAR_RAW';
            SET v_processed_table = 'T_RPA_CAR_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_processed_table = 'T_RPA_GENERAL_PROCESSED';
        END IF;

        -- Mapping for NEW contracts (Columns 01-29 shared LTR/CAR/GEN)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 영수일
            'COLUMN_02, ', -- 입력일
            'COLUMN_03, '); -- 책임개시일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 영수일
            'COLUMN_02, ', -- 입력일
            'COLUMN_03, '); -- 책임개시일

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 보험만기일
            'COLUMN_05, ', -- 3레벨대리점명
            'COLUMN_06, '); -- 2레벨대리점명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 보험만기일
            'COLUMN_05, ', -- 3레벨대리점명
            'COLUMN_06, '); -- 2레벨대리점명

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 1레벨대리점명
            'COLUMN_08, ', -- 대표대리점명
            'COLUMN_09, '); -- 성명/상호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 1레벨대리점명
            'COLUMN_08, ', -- 대표대리점명
            'COLUMN_09, '); -- 성명/상호

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 번호
            'COLUMN_11, ', -- 상품코드
            'COLUMN_12, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 번호
            'COLUMN_11, ', -- 상품코드
            'COLUMN_12, '); -- 상품명

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 보종
            'COLUMN_14, ', -- 세부구분
            'COLUMN_15, '); -- 증권번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 보종
            'COLUMN_14, ', -- 세부구분
            'COLUMN_15, '); -- 증권번호

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 계약자
            'COLUMN_17, ', -- 피보험자
            'COLUMN_18, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 계약자
            'COLUMN_17, ', -- 피보험자
            'COLUMN_18, '); -- 보험료

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보장성보험료
            'COLUMN_20, ', -- 신규수정보험료
            'COLUMN_21, '); -- 평가수정보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보장성보험료
            'COLUMN_20, ', -- 신규수정보험료
            'COLUMN_21, '); -- 평가수정보험료

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 납입방법
            'COLUMN_23, ', -- 회차
            'COLUMN_24, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 납입방법
            'COLUMN_23, ', -- 회차
            'COLUMN_24, '); -- 납입기간

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 본인계약유무
            'COLUMN_26, ', -- 상태
            'COLUMN_27, '); -- 신계약가치(NCEV)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 본인계약유무
            'COLUMN_26, ', -- 상태
            'COLUMN_27, '); -- 신계약가치(NCEV)

        -- 28-29
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 신계약가치(NCEV)상품군
            'COLUMN_29, '); -- 차량번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 신계약가치(NCEV)상품군
            'COLUMN_29, '); -- 차량번호

        -- 30-32 (Target-only)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = CONCAT(v_raw_cols,
                'NULL, ', -- 납기구분
                'NULL, ', -- 납입월
                'NULL');  -- 납입일
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_30, ', -- 납기구분
                'COLUMN_31, ', -- 납입월
                'COLUMN_32');  -- 납입일
        ELSE
            SET v_raw_cols = CONCAT(v_raw_cols,
                'NULL, ', -- 납입월
                'NULL');  -- 납입일
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_30, ', -- 납입월
                'COLUMN_31');  -- 납입일
        END IF;

    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' AND v_raw_table != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_DBG_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_DBG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- [DEBUG] Capture initial counts
        SET @sql_raw_count = CONCAT(
            'SELECT COUNT(*) INTO @v_raw_count FROM ', v_raw_table,
            ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') AND COMPANY_CODE = ''DBG'''
        );
        PREPARE stmt_raw FROM @sql_raw_count;
        EXECUTE stmt_raw;
        DEALLOCATE PREPARE stmt_raw;
        SET v_log_initial_raw = @v_raw_count;

        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_DBG_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 2.3. Apply transformation rules (LTR / CAR / GEN)

        -- [LTR Logic]
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'ENTER_LTR_BLOCK', v_log_temp_initial, NOW());

            /* Rule 1: 맨 마지막열 값 추가(3개)
               ① 항목명I : 납기구분 / 항목값 : 년납
               ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
               ③ 항목명III : 납입일 / 항목값 : 영수일과 동일한 값으로 반영
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_30 = '년납',
                COLUMN_31 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_32 = COLUMN_01;

            /* Rule 2: [증권번호] 오름차순 정렬 후
               [상태]≠"정상,철회,해지"이면 데이터 행삭제
            */
            -- Rule 2①: 증권번호 오름차순 정렬
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;
            CREATE TEMPORARY TABLE tmp_sorted_dbg LIKE T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO tmp_sorted_dbg SELECT * FROM T_TEMP_RPA_DBG_PROCESSED ORDER BY COLUMN_15 ASC;
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_TEMP_RPA_DBG_PROCESSED SELECT * FROM tmp_sorted_dbg;
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;

            SET @seq = 0;
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_15 ASC;

            -- Rule 2②: 상태 필터링
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE COLUMN_26 NOT IN ('정상', '철회', '해지');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE2', v_log_after_rule2, NOW());

            /* Rule 3: 중복 증권번호의 [상태]=각각"철회,정상"이면
               [상태]="철회"로 수정 및 [보험료]="마이너스금액" 데이터 행삭제
            */
            DROP TEMPORARY TABLE IF EXISTS tmp_mix_cancel;
            CREATE TEMPORARY TABLE tmp_mix_cancel
            SELECT COLUMN_15
            FROM T_TEMP_RPA_DBG_PROCESSED
            GROUP BY COLUMN_15
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_26 = '철회' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_26 = '정상' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_26 = '철회'
            WHERE COLUMN_15 IN (SELECT COLUMN_15 FROM tmp_mix_cancel);

            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE COLUMN_15 IN (SELECT COLUMN_15 FROM tmp_mix_cancel)
              AND CAST(REPLACE(IFNULL(COLUMN_18, '0'), ',', '') AS SIGNED) < 0;

            DROP TEMPORARY TABLE IF EXISTS tmp_mix_cancel;

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE3', v_log_after_rule3, NOW());

            /* Rule 4: [보험료]="마이너스금액"이면 "플러스"로 변경 */
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_18 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_18, '0'), ',', '') AS SIGNED)) AS CHAR)
            WHERE COLUMN_18 LIKE '-%';

            /* Rule 5: [신규수정보험료]="마이너스금액"이면 "플러스"로 변경 */
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_20 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_20, '0'), ',', '') AS SIGNED)) AS CHAR)
            WHERE COLUMN_20 LIKE '-%';

            SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE4_5', v_log_after_rule4, NOW());

            /* Rule 6: [책임개시일]≠해당월이면 데이터 행삭제 */
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE LEFT(COLUMN_03, 6) != DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE6', v_log_after_rule2, NOW());

        -- [CAR Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'ENTER_CAR_BLOCK', v_log_temp_initial, NOW());

            /* Rule 1: 맨 마지막열 값 추가(2개)
               ① 항목명I : 납입월 / 항목값 : 해당월(ex.202512)
               ② 항목명II : 납입일 / 항목값 : 영수일과 동일한 값으로 반영
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_30 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_31 = COLUMN_01;

            /* Rule 2: [영수일]="빈값"이면 데이터 행삭제 */
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE COLUMN_01 IS NULL OR COLUMN_01 = '';

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_AFTER_RULE2', v_log_after_rule2, NOW());

            /* Rule 3: [증권번호] 오름차순 정렬 후
               [상태]="계속,추징,추징/이체"이면 데이터 행삭제
            */
            -- Rule 3①: 증권번호 오름차순 정렬
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;
            CREATE TEMPORARY TABLE tmp_sorted_dbg LIKE T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO tmp_sorted_dbg SELECT * FROM T_TEMP_RPA_DBG_PROCESSED ORDER BY COLUMN_15 ASC;
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_TEMP_RPA_DBG_PROCESSED SELECT * FROM tmp_sorted_dbg;
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;

            SET @seq = 0;
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_15 ASC;

            -- Rule 3②: 상태 필터링
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE COLUMN_26 IN ('계속', '추징', '추징/이체');

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_AFTER_RULE3', v_log_after_rule3, NOW());

            /* Rule 4: 중복 증권번호의 [상태]=각각"취소,정상"이면
               [상태]="취소"로 값수정 및 [보험료]="마이너스금액"이면 데이터 행삭제
            */
            DROP TEMPORARY TABLE IF EXISTS tmp_mix_cancel;
            CREATE TEMPORARY TABLE tmp_mix_cancel
            SELECT COLUMN_15
            FROM T_TEMP_RPA_DBG_PROCESSED
            GROUP BY COLUMN_15
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_26 = '취소' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_26 = '정상' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_26 = '취소'
            WHERE COLUMN_15 IN (SELECT COLUMN_15 FROM tmp_mix_cancel);

            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE COLUMN_15 IN (SELECT COLUMN_15 FROM tmp_mix_cancel)
              AND CAST(REPLACE(IFNULL(COLUMN_18, '0'), ',', '') AS SIGNED) < 0;

            DROP TEMPORARY TABLE IF EXISTS tmp_mix_cancel;

            SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_AFTER_RULE4', v_log_after_rule4, NOW());

            /* Rule 5: 중복 증권번호의 [상태]=모두"정상"이면 변경안함
               ※ DB손보 자동차계약은 동일증번으로 세부구분이 책임일반/자동차 임의갱신으로
                  두건씩 발생하기도 함(철회건은 4건도 발생)
               → 모두 정상이면 그대로 유지 (No action required)
               → Rule 4에서 취소+정상 혼재 케이스만 처리하므로
                  모두 정상인 케이스는 자동으로 유지됨
            */

            /* Rule 6: [납입방법]≠"월납,일시납"이면 원수사 원부확인하여 값수정
               [PAUSE/SKIP] 수동 처리 필요 - SP에서 자동화 불가
            */

        -- [GEN Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'ENTER_GEN_BLOCK', v_log_temp_initial, NOW());

            /* Rule 1: 맨 마지막열 값 추가(2개)
               ① 항목명I : 납입월 / 항목값 : 해당월(ex.202512)
               ② 항목명II : 납입일 / 항목값 : 영수일과 동일한 값으로 반영
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_30 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_31 = COLUMN_01;

            /* Rule 2: [증권번호] 오름차순 정렬 후
               [상태]="계속,추징"이면 데이터 행삭제
            */
            -- Rule 2①: 증권번호 오름차순 정렬
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;
            CREATE TEMPORARY TABLE tmp_sorted_dbg LIKE T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO tmp_sorted_dbg SELECT * FROM T_TEMP_RPA_DBG_PROCESSED ORDER BY COLUMN_15 ASC;
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_TEMP_RPA_DBG_PROCESSED SELECT * FROM tmp_sorted_dbg;
            DROP TEMPORARY TABLE IF EXISTS tmp_sorted_dbg;

            SET @seq = 0;
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_15 ASC;

            -- Rule 2②: 상태 필터링
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED
            WHERE COLUMN_26 IN ('계속', '추징');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'GEN_AFTER_RULE2', v_log_after_rule2, NOW());

            /* Rule 3: [영수일]="빈값" & [입력일]="당월"이면 [영수일]=[입력일]로 값수정 */
            UPDATE T_TEMP_RPA_DBG_PROCESSED
            SET COLUMN_01 = COLUMN_02
            WHERE (COLUMN_01 IS NULL OR COLUMN_01 = '')
              AND LEFT(COLUMN_02, 6) = DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_DBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'GEN_AFTER_RULE3', v_log_after_rule3, NOW());

            /* Rule 4: [납입방법]≠"월납,일시납"이면 원수사 원부확인하여 값수정
               [PAUSE/SKIP] 수동 처리 필요 - SP에서 자동화 불가
            */

        END IF;

        -- [DEBUG] Count before final insert
        SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_DBG_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'BEFORE_FINAL_INSERT', v_row_count, NOW());

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_DBG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'FINAL_INSERT', v_row_count, NOW());

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBG_PROCESSED;

    END IF;

END