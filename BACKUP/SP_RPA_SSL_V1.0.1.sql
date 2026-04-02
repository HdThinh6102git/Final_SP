CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_SSL`(
    IN IN_BATCH_ID VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE VARCHAR(20)
)
BEGIN
    DECLARE v_raw_cols TEXT DEFAULT '';
    DECLARE v_proc_cols TEXT DEFAULT '';
    DECLARE v_sql_query TEXT DEFAULT '';
    DECLARE v_row_count INT DEFAULT 0;
    
    -- 1. Hardcoded Column Mapping for Samsung Life (SSL)
    -- Instead of selecting from T_RPA_RAW_META, we use fixed mapping for performance and simplicity.
    
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-28 + Target-only 29)
        -- Chunks of 3 columns for better maintainability
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 실납입월
            'COLUMN_02, ', -- 실납입보험료
            'COLUMN_03, '); -- 등록주소지
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 실납입월
            'COLUMN_02, ', -- 실납입보험료
            'COLUMN_03, '); -- 등록주소지
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 대리점
            'COLUMN_05, ', -- 대리점코드
            'COLUMN_06, '); -- 대리점명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 대리점
            'COLUMN_05, ', -- 대리점코드
            'COLUMN_06, '); -- 대리점명
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 자동이체일
            'COLUMN_08, ', -- 모집설계사코드
            'COLUMN_09, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 자동이체일
            'COLUMN_08, ', -- 모집설계사코드
            'COLUMN_09, '); -- 지점
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 계약번호
            'COLUMN_11, ', -- 계약자명
            'COLUMN_12, '); -- 피보험자명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 계약번호
            'COLUMN_11, ', -- 계약자명
            'COLUMN_12, '); -- 피보험자명
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 상품명
            'COLUMN_14, ', -- 계약일자
            'COLUMN_15, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 상품명
            'COLUMN_14, ', -- 계약일자
            'COLUMN_15, '); -- 최종납입년월
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 합계보험료
            'COLUMN_17, ', -- 모집환산
            'COLUMN_18, '); -- 납입기간(년)
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 합계보험료
            'COLUMN_17, ', -- 모집환산
            'COLUMN_18, '); -- 납입기간(년)
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- [기타]
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- [기타]
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- [기타]
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 보험계약상태
            'COLUMN_23, ', -- [기타]
            'COLUMN_24, '); -- 종납일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 보험계약상태
            'COLUMN_23, ', -- [기타]
            'COLUMN_24, '); -- 종납일자
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 종납횟수
            'COLUMN_26, ', -- [기타]
            'COLUMN_27, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 종납횟수
            'COLUMN_26, ', -- [기타]
            'COLUMN_27, '); -- [기타]
        
        -- 28-29
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- [기타]
            'NULL'); -- NULL
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- [기타]
            'COLUMN_29'); -- 납기구분
    
    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-28)
        -- Chunks of 3 columns for better maintainability
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 실납입월
            'COLUMN_02, ', -- 실납입보험료
            'COLUMN_03, '); -- 등록주소지
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 실납입월
            'COLUMN_02, ', -- 실납입보험료
            'COLUMN_03, '); -- 등록주소지
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 대리점
            'COLUMN_05, ', -- 대리점코드
            'COLUMN_06, '); -- 대리점명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 대리점
            'COLUMN_05, ', -- 대리점코드
            'COLUMN_06, '); -- 대리점명
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 자동이체일
            'COLUMN_08, ', -- 자동이체희망일
            'COLUMN_09, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 자동이체일
            'COLUMN_08, ', -- 자동이체희망일
            'COLUMN_09, '); -- 지점
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 계약번호
            'COLUMN_11, ', -- 지점명
            'COLUMN_12, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 계약번호
            'COLUMN_11, ', -- 지점명
            'COLUMN_12, '); -- [기타]
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- [기타]
            'COLUMN_14, ', -- 최종납입일자?
            'COLUMN_15, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- [기타]
            'COLUMN_14, ', -- 최종납입일자?
            'COLUMN_15, '); -- 최종납입년월
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 합계보험료
            'COLUMN_17, ', -- [기타]
            'COLUMN_18, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 합계보험료
            'COLUMN_17, ', -- [기타]
            'COLUMN_18, '); -- [기타]
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- [기타]
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- [기타]
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- [기타]
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 보험계약상태
            'COLUMN_23, ', -- 소멸일자
            'COLUMN_24, '); -- 종납일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 보험계약상태
            'COLUMN_23, ', -- 소멸일자
            'COLUMN_24, '); -- 종납일자
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 종납횟수
            'COLUMN_26, ', -- 유지년월?
            'COLUMN_27, '); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 종납횟수
            'COLUMN_26, ', -- 유지년월?
            'COLUMN_27, '); -- [기타]
        
        -- 28
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28'); -- [기타]
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28'); -- [기타]
    END IF;
      
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        -- 2. Create Temporary Table LIKE Processed table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
        CREATE TEMPORARY TABLE T_TEMP_RPA_LIFE_PROCESSED LIKE T_RPA_LIFE_PROCESSED;

        -- 3. Insert into Temporary Table using dynamic mapping from Raw table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''SSL'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM T_RPA_LIFE_RAW ',
            'WHERE COMPANY_CODE = ''SSL'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 4. Apply Transformation Logic in temporary table (Logic from SP_SSL_CORRECT_LOGIC)
        
        -- Logic for NEW Contract Type
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule: 납기구분 고정값 '년납'
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_29 = '년납'
            WHERE UPPER(CONTRACT_TYPE) = 'NEW';
        END IF;

        -- Logic for EXT Contract Type
        IF UPPER(IN_CONTRACT_TYPE) = 'EXT' OR UPPER(IN_CONTRACT_TYPE) = 'EXISTING' THEN
            -- Rule 1: 계약번호 정규화 (Normalization)
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_10 = CASE
                WHEN CHAR_LENGTH(REGEXP_REPLACE(COLUMN_10, '[^0-9]', '')) IN (10, 14)
                    THEN CONCAT('000', REGEXP_REPLACE(COLUMN_10, '[^0-9]', ''))
                WHEN CHAR_LENGTH(REGEXP_REPLACE(COLUMN_10, '[^0-9]', '')) = 12
                    THEN CONCAT('0', REGEXP_REPLACE(COLUMN_10, '[^0-9]', ''))
                ELSE REGEXP_REPLACE(COLUMN_10, '[^0-9]', '')
            END
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

            -- Rule 2: 종납일자 Update based on status (Ban-song / Cheol-hui)
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_24 = COLUMN_14
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND COLUMN_24 = '0000-00-00'
              AND (COLUMN_22 LIKE '%반송%' OR COLUMN_22 LIKE '%철회%');

            -- Rule 3.1: Final Payment Year/Month - Empty or Null handling
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_15 = DATE_FORMAT(COLUMN_14, '%Y-%m')
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND (COLUMN_15 IS NULL OR COLUMN_15 = '')
              AND (COLUMN_22 LIKE '%반송%' OR COLUMN_22 LIKE '%철회%');

            -- Rule 3.2: Final Payment Year/Month - Normal status adjustment
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_15 = COLUMN_26
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND COLUMN_15 = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 MONTH), '%Y-%m')
              AND COLUMN_22 LIKE '%정상%';

            -- Rule 3.3: Final Payment Year/Month - 6-month or 12-month payments
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_15 = COLUMN_26
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND COLUMN_20 IN ('6월납', '12월납');

            -- Rule 4: Payment Count 0 -> 1
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_25 = '1'
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND COLUMN_25 = '0';

            -- Rule 5: One-time payment (Il-si-nap) -> Total Premium 0, Count 1
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_16 = '0',
                COLUMN_25 = '1'
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND COLUMN_20 = '일시납';

            -- Rule 6: Group Welfare / Group Insurance + Cancellation (Hae-ji)
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_15 = LEFT(COLUMN_23, 7)
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND (COLUMN_13 LIKE '%기업복지%' OR COLUMN_13 LIKE '%단체보장%')
              AND COLUMN_22 = '해지'
              AND DATE_FORMAT(COLUMN_23, '%Y%m') <= REPLACE(COLUMN_15, '-', '');

            -- Rule 7: Status Effective -> Statute of Limitations (38 months)
            UPDATE T_TEMP_RPA_LIFE_PROCESSED
            SET COLUMN_22 = '시효'
            WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE)
              AND COLUMN_22 = '실효'
              AND PERIOD_DIFF(
                    DATE_FORMAT(CURDATE(), '%Y%m'),
                    REPLACE(COLUMN_15, '-', '')
                  ) >= 38;
        END IF;

        -- 5. Insert data from temp table into the processed table
        INSERT INTO T_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT,
            COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05,
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15,
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25,
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29
        )
        SELECT 
            SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(),
            COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05,
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15,
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25,
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29
        FROM T_TEMP_RPA_LIFE_PROCESSED ORDER BY SORT_ORDER_NO ASC;
        SET v_row_count = ROW_COUNT();

        -- 6. Cleanup
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
    END IF;
END