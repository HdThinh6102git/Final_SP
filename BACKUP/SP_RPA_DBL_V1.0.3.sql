CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_DBL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols     TEXT         DEFAULT '';
    DECLARE v_proc_cols    TEXT         DEFAULT '';
    DECLARE v_sql_query    TEXT         DEFAULT '';
    DECLARE v_raw_table    VARCHAR(100) DEFAULT 'T_RPA_LIFE_RAW';
    DECLARE v_proc_table   VARCHAR(100) DEFAULT 'T_RPA_LIFE_PROCESSED';
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'DBL';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_log_initial_raw   INT DEFAULT 0;
    DECLARE v_log_temp_initial  INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- [DEBUG] Log exception
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'SQL_EXCEPTION_TRIGGERED', 0, NOW());
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBL_PROCESSED;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Rule 5 cutoff: 38 months
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Hardcoded Column Mapping for DB Life (DBL)
    IF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-30)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 증권번호
            'COLUMN_02, ', -- 계약자
            'COLUMN_03, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 증권번호
            'COLUMN_02, ', -- 계약자
            'COLUMN_03, '); -- 보험료
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 납입횟수
            'COLUMN_05, ', -- 계약년월
            'COLUMN_06, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 납입횟수
            'COLUMN_05, ', -- 계약년월
            'COLUMN_06, '); -- 계약일자
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 종납년월
            'COLUMN_08, ', -- UV종납년월
            'COLUMN_09, '); -- UV종납횟수
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 종납년월
            'COLUMN_08, ', -- UV종납년월
            'COLUMN_09, '); -- UV종납횟수
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 모집코드
            'COLUMN_11, ', -- 모집사원명
            'COLUMN_12, '); -- 환산보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 모집코드
            'COLUMN_11, ', -- 모집사원명
            'COLUMN_12, '); -- 환산보험료
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 1차환산P
            'COLUMN_14, ', -- 2차환산P
            'COLUMN_15, '); -- 3차환산P
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 1차환산P
            'COLUMN_14, ', -- 2차환산P
            'COLUMN_15, '); -- 3차환산P
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 4차환산P
            'COLUMN_17, ', -- 보험종류
            'COLUMN_18, '); -- 상태
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 4차환산P
            'COLUMN_17, ', -- 보험종류
            'COLUMN_18, '); -- 상태
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 소멸일자
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 소멸일자
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입기간
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 주계약보종명
            'COLUMN_24, '); -- 수금사원코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 수금방법
            'COLUMN_23, ', -- 주계약보종명
            'COLUMN_24, '); -- 수금사원코드
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 수금사원명
            'COLUMN_26, ', -- 최종납입일자
            'COLUMN_27, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 수금사원명
            'COLUMN_26, ', -- 최종납입일자
            'COLUMN_27, '); -- 지점
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 지사
            'COLUMN_29, ', -- 생성일시
            'COLUMN_30'); -- 조회구분
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 지사
            'COLUMN_29, ', -- 생성일시
            'COLUMN_30'); -- 조회구분

        -- [DEBUG] Initialize Debug Log Table
        CREATE TABLE IF NOT EXISTS T_RPA_DEBUG_LOG (
            BATCH_ID VARCHAR(100),
            COMPANY_CODE VARCHAR(10),
            INSURANCE_TYPE VARCHAR(50),
            CONTRACT_TYPE VARCHAR(20),
            STEP_NAME VARCHAR(100),
            ROW_COUNT INT,
            LOG_TIME DATETIME
        );

        -- [DEBUG] Record INITIAL_RAW
        SELECT COUNT(*) INTO v_log_initial_raw FROM T_RPA_LIFE_RAW 
        WHERE BATCH_ID = IN_BATCH_ID AND COMPANY_CODE = 'DBL' AND UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBL_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_DBL_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_DBL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''DBL'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''DBL'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_01 <> ''증권번호'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- [DEBUG] Record TEMP_INITIAL
        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_DBL_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
            -- Rule 2: [상태]=실효,연체,완납,정상 & [UV종납년월]=값있음 이면
            -- ① [종납년월]값을 [UV종납년월]로 수정
            -- ② [납입횟수]값을 [UV 납입회차]로 수정
            UPDATE T_TEMP_RPA_DBL_PROCESSED
            SET COLUMN_07 = COLUMN_08, COLUMN_04 = COLUMN_09
            WHERE COLUMN_18 IN ('실효', '연체', '완납', '정상') AND COLUMN_08 IS NOT NULL AND COLUMN_08 <> '';
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'EXT_AFTER_RULE2_UPDATES', (SELECT COUNT(*) FROM T_TEMP_RPA_DBL_PROCESSED), NOW());

            -- Rule 3: [상태]=실효,연체,완납,정상이면
            --  [소멸일자]값을 “0000-00-00”으로 수정
            UPDATE T_TEMP_RPA_DBL_PROCESSED SET COLUMN_19 = '0000-00-00'
            WHERE COLUMN_18 IN ('실효', '연체', '완납', '정상');
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'EXT_AFTER_RULE3_UPDATES', (SELECT COUNT(*) FROM T_TEMP_RPA_DBL_PROCESSED), NOW());

            -- Rule 4: [납입주기]=“일시납”이면
            --  [보험료]=“0”으로 수정
            --  [납입횟수]=“1”로 수정
            UPDATE T_TEMP_RPA_DBL_PROCESSED SET COLUMN_03 = '0', COLUMN_04 = '1'
            WHERE COLUMN_20 = '일시납';
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'EXT_AFTER_RULE4_UPDATES', (SELECT COUNT(*) FROM T_TEMP_RPA_DBL_PROCESSED), NOW());

            -- Rule 5:  [상태]=실효 & [최종납입월]=실효 3년 경과면, [상태]값을 “시효＂로 변경
            -- 3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하
            UPDATE T_TEMP_RPA_DBL_PROCESSED SET COLUMN_18 = '시효'
            WHERE COLUMN_18 = '실효'
              AND COLUMN_26 IS NOT NULL AND COLUMN_26 <> ''
              AND LEFT(REPLACE(REPLACE(COLUMN_26, '-', ''), '.', ''), 6) <= v_cutoff_ym;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'EXT_AFTER_RULE5_UPDATES', (SELECT COUNT(*) FROM T_TEMP_RPA_DBL_PROCESSED), NOW());
        END IF;

        -- [DEBUG] Record final state
        SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_DBL_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'DBL', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'BEFORE_FINAL_INSERT', v_row_count, NOW());

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_DBL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBL_PROCESSED;

    END IF;

END
