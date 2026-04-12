/*
 * SP_RPA_HWG
 * Description : Process Hanwha Fire insurance data
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

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_HWG`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'HWG';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT '';
    DECLARE v_processed_table VARCHAR(100) DEFAULT '';

    -- [DECLARE debug variables]
    DECLARE v_log_initial_raw  INT DEFAULT 0;
    DECLARE v_log_temp_initial INT DEFAULT 0;
    DECLARE v_log_after_rule1  INT DEFAULT 0;
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
            IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
            CONCAT('SQL_EXCEPTION: [', @v_err_no, '] ', @v_err_msg),
            0, NOW()
        );
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HWG_PROCESSED;
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
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';

        -- Mapping for LTR contracts (Columns 01-36 + Target-only 37-39)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 순번
            'COLUMN_02, ', -- 소속기관
            'COLUMN_03, '); -- 팀기관
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 순번
            'COLUMN_02, ', -- 소속기관
            'COLUMN_03, '); -- 팀기관

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 취급자코드
            'COLUMN_05, ', -- 취급자
            'COLUMN_06, '); -- 사용인코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 취급자코드
            'COLUMN_05, ', -- 취급자
            'COLUMN_06, '); -- 사용인코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 사용인
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 사용인
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 상품명

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 계약자
            'COLUMN_11, ', -- 피보험자
            'COLUMN_12, '); -- 계상일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 계약자
            'COLUMN_11, ', -- 피보험자
            'COLUMN_12, '); -- 계상일자

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 보험시기
            'COLUMN_14, ', -- 실납입기간
            'COLUMN_15, '); -- 수금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 보험시기
            'COLUMN_14, ', -- 실납입기간
            'COLUMN_15, '); -- 수금방법

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 납입주기
            'COLUMN_17, ', -- 계약상태명
            'COLUMN_18, '); -- 월납환산보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 납입주기
            'COLUMN_17, ', -- 계약상태명
            'COLUMN_18, '); -- 월납환산보험료

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 영수보험료
            'COLUMN_20, ', -- 보장보험료
            'COLUMN_21, '); -- 환산실적
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 영수보험료
            'COLUMN_20, ', -- 보장보험료
            'COLUMN_21, '); -- 환산실적

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 핸드폰
            'COLUMN_23, ', -- 핸드폰뒷자리
            'COLUMN_24, '); -- 자택연락처
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 핸드폰
            'COLUMN_23, ', -- 핸드폰뒷자리
            'COLUMN_24, '); -- 자택연락처

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 자택연락처뒷자리
            'COLUMN_26, ', -- 증권발행여부
            'COLUMN_27, '); -- 청약형태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 자택연락처뒷자리
            'COLUMN_26, ', -- 증권발행여부
            'COLUMN_27, '); -- 청약형태

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 만기구분
            'COLUMN_29, ', -- 상품코드
            'COLUMN_30, '); -- 상품분류
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 만기구분
            'COLUMN_29, ', -- 상품코드
            'COLUMN_30, '); -- 상품분류

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 서명방법
            'COLUMN_32, ', -- 보험종목코드
            'COLUMN_33, '); -- 실손갱신증번
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 서명방법
            'COLUMN_32, ', -- 보험종목코드
            'COLUMN_33, '); -- 실손갱신증번

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 본인계약여부
            'COLUMN_35, ', -- 자녀보험선지급형여부
            'COLUMN_36, '); -- 플랜명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 본인계약여부
            'COLUMN_35, ', -- 자녀보험선지급형여부
            'COLUMN_36, '); -- 플랜명

        -- 37-39 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납기구분
            'NULL, ', -- 납입월
            'NULL');  -- 납입일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 납기구분
            'COLUMN_38, ', -- 납입월
            'COLUMN_39');  -- 납입일

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        SET v_raw_table = 'T_RPA_CAR_RAW';
        SET v_processed_table = 'T_RPA_CAR_PROCESSED';

        -- Mapping for CAR contracts (Columns 01-31 + Target-only 32-35)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 순번
            'COLUMN_02, ', -- 취급자코드
            'COLUMN_03, '); -- 취급자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 순번
            'COLUMN_02, ', -- 취급자코드
            'COLUMN_03, '); -- 취급자

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인
            'COLUMN_06, '); -- 팀기관
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 사용인코드
            'COLUMN_05, ', -- 사용인
            'COLUMN_06, '); -- 팀기관

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 피보험자
            'COLUMN_08, ', -- 계약자
            'COLUMN_09, '); -- 증권번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 피보험자
            'COLUMN_08, ', -- 계약자
            'COLUMN_09, '); -- 증권번호

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 차량번호
            'COLUMN_11, ', -- 보험상품
            'COLUMN_12, '); -- 플랜형
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 차량번호
            'COLUMN_11, ', -- 보험상품
            'COLUMN_12, '); -- 플랜형

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 마일리지특약
            'COLUMN_14, ', -- 보험시기
            'COLUMN_15, '); -- 보험종기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 마일리지특약
            'COLUMN_14, ', -- 보험시기
            'COLUMN_15, '); -- 보험종기

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 표준율
            'COLUMN_17, ', -- 발생구분
            'COLUMN_18, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 표준율
            'COLUMN_17, ', -- 발생구분
            'COLUMN_18, '); -- 납입회차

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보험료
            'COLUMN_20, ', -- 계상일자
            'COLUMN_21, '); -- 영수일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보험료
            'COLUMN_20, ', -- 계상일자
            'COLUMN_21, '); -- 영수일자

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 금종구분
            'COLUMN_23, ', -- 계약구분
            'COLUMN_24, '); -- 자율여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 금종구분
            'COLUMN_23, ', -- 계약구분
            'COLUMN_24, '); -- 자율여부

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 가입구분
            'COLUMN_26, ', -- 물건구분
            'COLUMN_27, '); -- 스캔여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 가입구분
            'COLUMN_26, ', -- 물건구분
            'COLUMN_27, '); -- 스캔여부

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 비례소득
            'COLUMN_29, ', -- 청약형태
            'COLUMN_30, '); -- 전자서명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 비례소득
            'COLUMN_29, ', -- 청약형태
            'COLUMN_30, '); -- 전자서명

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 영업속성
            'NULL, ',      -- 납기구분
            'NULL, ');     -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 영업속성
            'COLUMN_32, ', -- 납기구분
            'COLUMN_33, '); -- 납입월

        -- 34-35 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납기
            'NULL');  -- 납입주기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 납기
            'COLUMN_35');  -- 납입주기

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        SET v_raw_table = 'T_RPA_GENERAL_RAW';
        SET v_processed_table = 'T_RPA_GENERAL_PROCESSED';

        -- Mapping for GEN contracts (Columns 01-24 + Target-only 25-27)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 순번
            'COLUMN_02, ', -- 모집자
            'COLUMN_03, '); -- 모집자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 순번
            'COLUMN_02, ', -- 모집자
            'COLUMN_03, '); -- 모집자명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 사용인
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 팀기관
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 사용인
            'COLUMN_05, ', -- 사용인명
            'COLUMN_06, '); -- 팀기관

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 보험종목
            'COLUMN_08, ', -- 상품명
            'COLUMN_09, '); -- 증권번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 보험종목
            'COLUMN_08, ', -- 상품명
            'COLUMN_09, '); -- 증권번호

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 상품코드
            'COLUMN_11, ', -- 계약자명
            'COLUMN_12, '); -- 보험시기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 상품코드
            'COLUMN_11, ', -- 계약자명
            'COLUMN_12, '); -- 보험시기

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 보험종기
            'COLUMN_14, ', -- 인수구분
            'COLUMN_15, '); -- 계상일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 보험종기
            'COLUMN_14, ', -- 인수구분
            'COLUMN_15, '); -- 계상일자

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 수납일자
            'COLUMN_17, ', -- 신규갱신
            'COLUMN_18, '); -- 청약형태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 수납일자
            'COLUMN_17, ', -- 신규갱신
            'COLUMN_18, '); -- 청약형태

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 납입방법
            'COLUMN_20, ', -- 발생구분
            'COLUMN_21, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 납입방법
            'COLUMN_20, ', -- 발생구분
            'COLUMN_21, '); -- 납입회차

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 보험료
            'COLUMN_23, ', -- 비례소득
            'COLUMN_24, '); -- 일반성과|합산성적반영율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 보험료
            'COLUMN_23, ', -- 비례소득
            'COLUMN_24, '); -- 일반성과|합산성적반영율

        -- 25-27 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납기구분
            'NULL, ', -- 납입월
            'NULL');  -- 납기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 납기구분
            'COLUMN_26, ', -- 납입월
            'COLUMN_27');  -- 납기
    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HWG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_HWG_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_HWG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
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
            ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' ',
            'AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            'AND COMPANY_CODE = ''HWG'''
        );
        PREPARE stmt_raw FROM @sql_raw_count;
        EXECUTE stmt_raw;
        DEALLOCATE PREPARE stmt_raw;
        SET v_log_initial_raw = @v_raw_count;

        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_HWG_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 2.3. Apply transformation rules (LTR / CAR / GEN)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 맨 마지막열 값 추가(3개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납입일 / 항목값 : 계상일자와 동일한 값으로 반영
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_HWG_PROCESSED
            SET COLUMN_37 = '년납',
                COLUMN_38 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_39 = COLUMN_12;

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: [월납환산보험료]="마이너스금액" 데이터 행삭제
            DELETE FROM T_TEMP_RPA_HWG_PROCESSED
            WHERE COLUMN_18 IS NOT NULL
              AND REPLACE(COLUMN_18, ',', '') REGEXP '^-[0-9]+'
              AND CAST(REPLACE(COLUMN_18, ',', '') AS SIGNED) < 0;

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 맨 마지막열 값 추가(4개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납기 / 항목값 : 0
            -- ④ 항목명IV : 납입주기 / 항목값 : 일시납
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_HWG_PROCESSED
            SET COLUMN_32 = '년납',
                COLUMN_33 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_34 = '0',
                COLUMN_35 = '일시납';

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: [발생구분]="추징, 환급"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_HWG_PROCESSED
            WHERE COLUMN_17 IN ('추징', '환급');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            -- Rule 3: [보험료]="마이너스 금액"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_HWG_PROCESSED
            WHERE COLUMN_19 IS NOT NULL
              AND REPLACE(COLUMN_19, ',', '') REGEXP '^-[0-9]+'
              AND CAST(REPLACE(COLUMN_19, ',', '') AS SIGNED) < 0;

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_3', v_log_after_rule3, NOW());

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 맨 마지막열 값 추가(3개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납기 / 항목값 : 0
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_HWG_PROCESSED
            SET COLUMN_25 = '년납',
                COLUMN_26 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_27 = '0';

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: [발생구분]="추징, 환급"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_HWG_PROCESSED
            WHERE COLUMN_20 IN ('추징', '환급');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            -- Rule 3: [발생구분]="해지" & [보험시기]≠"해당월"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_HWG_PROCESSED
            WHERE COLUMN_20 = '해지'
              AND LEFT(REPLACE(REPLACE(COLUMN_12, '-', ''), '.', ''), 6) <> DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_3', v_log_after_rule3, NOW());

            -- Rule 4: [보험료]="마이너스 금액"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_HWG_PROCESSED
            WHERE COLUMN_22 IS NOT NULL
              AND REPLACE(COLUMN_22, ',', '') REGEXP '^-[0-9]+'
              AND CAST(REPLACE(COLUMN_22, ',', '') AS SIGNED) < 0;

            SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_HWG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_4', v_log_after_rule4, NOW());

        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_HWG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HWG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'FINAL_INSERT', v_row_count, NOW());

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HWG_PROCESSED;

    END IF;

END