CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_HWG`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols     TEXT         DEFAULT '';
    DECLARE v_proc_cols    TEXT         DEFAULT '';
    DECLARE v_sql_query    TEXT         DEFAULT '';
    DECLARE v_raw_table    VARCHAR(100) DEFAULT '';
    DECLARE v_proc_table   VARCHAR(100) DEFAULT '';
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'HWG';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HWG_PROCESSED;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    -- 1. Hardcoded Column Mapping for Hanwha Fire (HWG)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        -- Mapping for LTR (Columns 01-38)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- -
                'COLUMN_02, ', -- -
                'COLUMN_03, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- -
                'COLUMN_02, ', -- -
                'COLUMN_03, '); -- -, -, -
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- -
                'COLUMN_05, ', -- -
                'COLUMN_06, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- -
                'COLUMN_05, ', -- -
                'COLUMN_06, '); -- -, -, -
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 증권번호
                'COLUMN_08, ', -- -
                'COLUMN_09, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 증권번호
                'COLUMN_08, ', -- -
                'COLUMN_09, '); -- -, 증권번호, -
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- 계상일자
                'COLUMN_12, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- 계상일자
                'COLUMN_12, '); -- -, 계상일자, -
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- -
                'COLUMN_15, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- -
                'COLUMN_15, '); -- -, -, -
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- 월납환산
                'COLUMN_18, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- 월납환산
                'COLUMN_18, '); -- -, 월납환산, -
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- -, -, -
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- -
                'COLUMN_23, ', -- -
                'COLUMN_24, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- -
                'COLUMN_23, ', -- -
                'COLUMN_24, '); -- -, -, -
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- -
                'COLUMN_26, ', -- -
                'COLUMN_27, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- -
                'COLUMN_26, ', -- -
                'COLUMN_27, '); -- -, -, -
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- -
                'COLUMN_29, ', -- -
                'COLUMN_30, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- -
                'COLUMN_29, ', -- -
                'COLUMN_30, '); -- -, -, -
            
            -- 31-33
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_31, ', -- -
                'COLUMN_32, ', -- -
                'COLUMN_33, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- -
                'COLUMN_32, ', -- -
                'COLUMN_33, '); -- -, -, -
            
            -- 34-36
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_34, ', -- -
                'COLUMN_35, ', -- -
                'NULL, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_34, ', -- -
                'COLUMN_35, ', -- -
                'COLUMN_36, '); -- -, -, 납기구분
            
            -- 37-38
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- -
                'NULL'); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_37, ', -- 납입월
                'COLUMN_38'); -- 납입월, 납입일

        -- Mapping for CAR (Columns 01-30 + Target-only 31, 32, 33, 34)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- -
                'COLUMN_02, ', -- -
                'COLUMN_03, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- -
                'COLUMN_02, ', -- -
                'COLUMN_03, '); -- -, -, -
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- -
                'COLUMN_05, ', -- -
                'COLUMN_06, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- -
                'COLUMN_05, ', -- -
                'COLUMN_06, '); -- -, -, -
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- -
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- -
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- -, 증권번호, -
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- -, -, -
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- -
                'COLUMN_15, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- -
                'COLUMN_15, '); -- -, -, -
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 발생구분
                'COLUMN_17, ', -- -
                'COLUMN_18, '); -- 보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 발생구분
                'COLUMN_17, ', -- -
                'COLUMN_18, '); -- 발생구분, -, 보험료
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- -, -, -
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- -
                'COLUMN_23, ', -- -
                'COLUMN_24, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- -
                'COLUMN_23, ', -- -
                'COLUMN_24, '); -- -, -, -
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- -
                'COLUMN_26, ', -- -
                'COLUMN_27, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- -
                'COLUMN_26, ', -- -
                'COLUMN_27, '); -- -, -, -
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- -
                'COLUMN_29, ', -- -
                'COLUMN_30, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- -
                'COLUMN_29, ', -- -
                'COLUMN_30, '); -- -, -, -
            
            -- 31-34
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- -
                'NULL, ', -- -
                'NULL, ', -- -
                'NULL'); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 납기구분
                'COLUMN_32, ', -- 납입월
                'COLUMN_33, ', -- 납기
                'COLUMN_34'); -- 납기구분, 납입월, 납기, 납입주기

        -- Mapping for GEN (Columns 01-20 + Target-only 21, 22, 23)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- -
                'COLUMN_02, ', -- -
                'COLUMN_03, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- -
                'COLUMN_02, ', -- -
                'COLUMN_03, '); -- -, -, -
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- -
                'COLUMN_05, ', -- -
                'COLUMN_06, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- -
                'COLUMN_05, ', -- -
                'COLUMN_06, '); -- -, -, -
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 증권번호
                'COLUMN_08, ', -- -
                'COLUMN_09, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 증권번호
                'COLUMN_08, ', -- -
                'COLUMN_09, '); -- -, 증권번호, -
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- -, -, -
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- -
                'COLUMN_15, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- -
                'COLUMN_15, '); -- -, -, -
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- -
                'COLUMN_18, '); -- 보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- -
                'COLUMN_18, '); -- -, -, 보험료
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 발생구분
                'COLUMN_20, ', -- -
                'NULL'); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 발생구분
                'COLUMN_20, ', -- -
                'COLUMN_21'); -- 발생구분, -, 납기구분
            
            -- 22-23
            SET v_raw_cols = CONCAT(v_raw_cols, 
                ', NULL, ', -- -
                'NULL'); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                ', COLUMN_22, ', -- 납입월
                'COLUMN_23'); -- 납입월, 납기
        END IF;

    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Select Tables based on Insurance Type
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
            SET v_proc_table = 'T_RPA_LONG_TERM_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_table = 'T_RPA_CAR_RAW';
            SET v_proc_table = 'T_RPA_CAR_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
        END IF;

        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HWG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_HWG_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_HWG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''HWG'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''HWG'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND (COLUMN_07 <> ''증권번호'' AND COLUMN_08 <> ''증권번호'');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            
            -- [LTR Logic]
            IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
                -- Rule 1: 납기구분='년납', 납입월=해당월, 납입일=계상일자(11)
                UPDATE T_TEMP_RPA_HWG_PROCESSED SET COLUMN_36 = '년납', COLUMN_37 = v_target_ym, COLUMN_38 = COLUMN_11;
                -- Rule 2: 월납환산보험료 음수 -> 행 삭제
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_17 IS NOT NULL AND REPLACE(COLUMN_17, ',', '') REGEXP '^-[0-9]+' AND CAST(REPLACE(COLUMN_17, ',', '') AS SIGNED) < 0;

            -- [CAR Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
                -- Rule 1: 납기구분='년납', 납입월=해당월, 납기='0', 납입주기='일시납'
                UPDATE T_TEMP_RPA_HWG_PROCESSED SET COLUMN_31 = '년납', COLUMN_32 = v_target_ym, COLUMN_33 = '0', COLUMN_34 = '일시납';
                -- Rule 2: 발생구분 IN '추징','환급' -> 행 삭제 / 보험료 음수 -> 행 삭제
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_16 IN ('추징', '환급');
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_18 IS NOT NULL AND REPLACE(COLUMN_18, ',', '') REGEXP '^-[0-9]+' AND CAST(REPLACE(COLUMN_18, ',', '') AS SIGNED) < 0;

            -- [GEN Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                -- Rule 1: 납기구분='년납', 납입월=해당월, 납기='0'
                UPDATE T_TEMP_RPA_HWG_PROCESSED SET COLUMN_21 = '년납', COLUMN_22 = v_target_ym, COLUMN_23 = '0';
                -- Rule 2: 발생구분 IN '추징','환급' -> 행 삭제 / 해지 & ≠해당월 -> 행 삭제 / 보험료 음수 -> 행 삭제
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_19 IN ('추징', '환급');
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_19 = '해지' AND LEFT(REPLACE(REPLACE(COLUMN_11, '-', ''), '.', ''), 6) <> v_target_ym;
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_18 IS NOT NULL AND REPLACE(COLUMN_18, ',', '') REGEXP '^-[0-9]+' AND CAST(REPLACE(COLUMN_18, ',', '') AS SIGNED) < 0;
            END IF;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_HWG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HWG_PROCESSED;

    END IF;

END
