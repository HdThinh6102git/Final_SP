/*
 * SP_RPA_KBL
 * Description : Process KB Life insurance data
 * Parameters  :
 *   IN IN_BATCH_ID       : Batch ID to process
 *   IN IN_INSURANCE_TYPE : Insurance type (LIFE)
 *   IN IN_CONTRACT_TYPE  : Contract type (NEW / EXISTING)
 * Steps       :
 *   1. Hardcoded column mapping by contract type
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_KBL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'KBL';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT 'T_RPA_LIFE_RAW';
    DECLARE v_processed_table VARCHAR(100) DEFAULT 'T_RPA_LIFE_PROCESSED';

    -- [DECLARE debug variables]
    DECLARE v_log_initial_raw  INT DEFAULT 0;
    DECLARE v_log_temp_initial INT DEFAULT 0;
    DECLARE v_log_after_rule1  INT DEFAULT 0;
    DECLARE v_log_after_rule2  INT DEFAULT 0;
    DECLARE v_log_after_rule3  INT DEFAULT 0;
    DECLARE v_log_after_rule4  INT DEFAULT 0;
    DECLARE v_log_after_rule5  INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @v_err_no = MYSQL_ERRNO,
            @v_err_msg = MESSAGE_TEXT;

        INSERT INTO T_RPA_DEBUG_LOG VALUES (
            IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
            CONCAT('SQL_EXCEPTION: [', @v_err_no, '] ', @v_err_msg),
            0, NOW()
        );

        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBL_PROCESSED;
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
    IF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-34 + Target-only 35)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)

        -- 34-35
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 환산성적(3차년도)
            'NULL');       -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 환산성적(3차년도)
            'COLUMN_35');  -- 납기구분

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'LIF'
       AND UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXISTING contracts (Columns 01-34)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 지점명
            'COLUMN_02, ', -- 수금설계사
            'COLUMN_03, '); -- 수금설계사코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 증권번호
            'COLUMN_05, ', -- 계약자
            'COLUMN_06, '); -- 피보험자

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 상품
            'COLUMN_08, ', -- 상품코드
            'COLUMN_09, '); -- 계약일자

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 상태
            'COLUMN_11, ', -- 계약상태
            'COLUMN_12, '); -- 보험료

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 실납입보험료
            'COLUMN_14, ', -- 보험료(달러)
            'COLUMN_15, '); -- 선수/선납보험료

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 최종납입월
            'COLUMN_17, ', -- 최종납입월(예정)
            'COLUMN_18, '); -- 최종횟수

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 최종횟수(예정)
            'COLUMN_20, ', -- 최종납입일
            'COLUMN_21, '); -- 차기납입일자

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 납입방법
            'COLUMN_24, '); -- 이체일

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 납입기간
            'COLUMN_26, ', -- 계약상태변경일
            'COLUMN_27, '); -- 만기/소멸일

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 확정회차
            'COLUMN_29, ', -- 모집설계사
            'COLUMN_30, '); -- 모집설계사코드

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 가입금액
            'COLUMN_32, ', -- 환산성적(초년도)
            'COLUMN_33, '); -- 환산성적(2차년도)

        -- 34
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34'); -- 환산성적(3차년도)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34'); -- 환산성적(3차년도)
    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBL_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_KBL_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_KBL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_04 <> ''증권번호'';'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- [DEBUG] Capture initial counts
        SET @sql_raw_count = CONCAT(
            'SELECT COUNT(*) INTO @v_raw_count FROM ', v_raw_table,
            ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' ',
            'AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            'AND COMPANY_CODE = ''', v_company_code, ''''
        );
        PREPARE stmt_raw FROM @sql_raw_count;
        EXECUTE stmt_raw;
        DEALLOCATE PREPARE stmt_raw;
        SET v_log_initial_raw = @v_raw_count;

        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_KBL_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 2.3. Apply transformation rules
        IF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 납입기간 필드값 중 “세”가 있는지 확인
            -- ① 세납건이 있는 경우 원부 확인하여 수정
            -- [PAUSE/SKIP] 청약관리 > 일자별신계약리스트 수동 확인 필요

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: 맨 마지막열 값 추가(1개)
            -- ① 항목명 : 납기구분 / 항목값 : 년납
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_35 = '년납';

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'LIF'
           AND UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN

            -- Rule 1: 계약상태변경일 편집
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_26 = '0000-00-00'
            WHERE COLUMN_10 IN ('계류', '정상');

            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_26 = '0000-00-00'
            WHERE COLUMN_10 = '종료'
              AND COLUMN_11 IN ('실효(환급금없는실효)', '실효(환급금있는실효)');

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: 최종 편집
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_16 = LEFT(COLUMN_09, 7)
            WHERE (COLUMN_16 IS NULL OR COLUMN_16 = '' OR COLUMN_16 = '1900-01')
              AND COLUMN_11 IN ('계류(성립이전)', '반송', '철회');

            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_18 = '1'
            WHERE (COLUMN_18 IS NULL OR COLUMN_18 = '' OR COLUMN_18 = '0')
              AND COLUMN_11 IN ('계류(성립이전)', '반송', '철회');

            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_20 = CASE
                                WHEN COLUMN_18 = '1' THEN COLUMN_09
                                ELSE '0000-00-00'
                            END
            WHERE COLUMN_20 IS NULL OR COLUMN_20 = '';

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            -- Rule 3: [납입방법]=“일시납”
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_12 = '0',
                COLUMN_18 = '1'
            WHERE COLUMN_23 = '일시납';

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_3', v_log_after_rule3, NOW());

            -- Rule 4: [계약상태]="신계약" & [최종납입월]=실효월 여부 판단해서 실효로 변경
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_11 = '실효'
            WHERE COLUMN_11 = '신계약'
              AND REPLACE(COLUMN_16, '-', '') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 MONTH), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_4', v_log_after_rule4, NOW());

            -- Rule 5: [계약상태]=“실효” & [최종납입월]=“실효 3년 경과”면, [계약상태]값을 “시효”로 변경
            UPDATE T_TEMP_RPA_KBL_PROCESSED
            SET COLUMN_11 = '시효'
            WHERE COLUMN_11 IN ('실효(환급금없는실효)', '실효(환급금있는실효)')
              AND COLUMN_16 IS NOT NULL
              AND COLUMN_16 <> ''
              AND REPLACE(LEFT(COLUMN_16, 7), '-', '') <= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 38 MONTH), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule5 FROM T_TEMP_RPA_KBL_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_5', v_log_after_rule5, NOW());

        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_KBL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'FINAL_INSERT', v_row_count, NOW());

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBL_PROCESSED;

    END IF;

END