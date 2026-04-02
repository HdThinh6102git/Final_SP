CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_DBG`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'DBG';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    -- 1. Hardcoded Column Mapping for DB Insurance (DBG)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        -- Mapping for LTR (Columns 01-29 + Target-only 30, 31, 32)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-05
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, ');
            
            -- 06-10
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, ');
            
            -- 11-15
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, ');
            
            -- 16-20
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, ');
            
            -- 21-25
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, ');
            
            -- 26-29
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, ');

            -- 30-32 (Target specific)
            SET v_raw_cols = CONCAT(v_raw_cols, 'NULL, NULL, NULL');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_30, COLUMN_31, COLUMN_32');

        -- Mapping for CAR/GEN (Columns 01-29 + Target-only 30, 31)
        ELSEIF UPPER(IN_INSURANCE_TYPE) IN ('CAR', 'GEN') THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-05
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, ');
            
            -- 06-10
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, ');
            
            -- 11-15
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, ');
            
            -- 16-20
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, ');
            
            -- 21-25
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, ');
            
            -- 26-29
            SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, ');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, ');

            -- 30-31 (Target specific)
            SET v_raw_cols = CONCAT(v_raw_cols, 'NULL, NULL');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_30, COLUMN_31');
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
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_DBG_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_DBG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''DBG'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''DBG'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_15 <> ''증권번호'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        -- [LTR Logic]
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납기구분 = 년납, 납입월 = 해당월, 납입일 = 영수일
            UPDATE T_TEMP_RPA_DBG_PROCESSED SET COLUMN_30 = '년납', COLUMN_31 = v_target_ym, COLUMN_32 = COLUMN_01;

            -- Rule 2: [상태]≠"정상, 철회, 해지" 삭제
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE COLUMN_26 NOT IN ('정상', '철회', '해지');

            -- Rule 3: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_15 FROM T_TEMP_RPA_DBG_PROCESSED GROUP BY COLUMN_15 HAVING SUM(COLUMN_26='철회')>0 AND SUM(COLUMN_26='정상')>0;
            UPDATE T_TEMP_RPA_DBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_15 = d.COLUMN_15 SET t.COLUMN_26 = '철회';
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE COLUMN_15 IN (SELECT COLUMN_15 FROM tmp_dup_case) AND (COLUMN_18 LIKE '-%' OR CAST(REPLACE(COLUMN_18,',','') AS SIGNED) < 0);

            -- Rule 4-5: 보험료 마이너스 -> 플러스
            UPDATE T_TEMP_RPA_DBG_PROCESSED SET COLUMN_18 = CAST(ABS(CAST(REPLACE(COLUMN_18,',','') AS SIGNED)) AS CHAR) WHERE COLUMN_18 LIKE '-%';
            UPDATE T_TEMP_RPA_DBG_PROCESSED SET COLUMN_20 = CAST(ABS(CAST(REPLACE(COLUMN_20,',','') AS SIGNED)) AS CHAR) WHERE COLUMN_20 LIKE '-%';

            -- Rule 6: [책임개시일]≠해당월 삭제
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE LEFT(REPLACE(REPLACE(COLUMN_03, '-', ''), '.', ''), 6) <> v_target_ym;

        -- [CAR Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납입월 = 해당월, 납입일 = 영수일
            UPDATE T_TEMP_RPA_DBG_PROCESSED SET COLUMN_30 = v_target_ym, COLUMN_31 = COLUMN_01;

            -- Rule 2: [영수일]="빈값" 삭제
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE COLUMN_01 IS NULL OR TRIM(COLUMN_01) = '';

            -- Rule 3: [상태]＝"계속,추징,추징/이체" 삭제
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE COLUMN_26 IN ('계속', '추징', '추징/이체');

            -- Rule 4: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_15 FROM T_TEMP_RPA_DBG_PROCESSED GROUP BY COLUMN_15 HAVING SUM(COLUMN_26='취소')>0 AND SUM(COLUMN_26='정상')>0;
            UPDATE T_TEMP_RPA_DBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_15 = d.COLUMN_15 SET t.COLUMN_26 = '취소';
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE COLUMN_15 IN (SELECT COLUMN_15 FROM tmp_dup_case) AND (COLUMN_18 LIKE '-%' OR CAST(REPLACE(COLUMN_18,',','') AS SIGNED) < 0);

        -- [GEN Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납입월 = 해당월, 납입일 = 영수일
            UPDATE T_TEMP_RPA_DBG_PROCESSED SET COLUMN_30 = v_target_ym, COLUMN_31 = COLUMN_01;

            -- Rule 2: [상태]＝"계속,추징" 삭제
            DELETE FROM T_TEMP_RPA_DBG_PROCESSED WHERE COLUMN_26 IN ('계속', '추징');

            -- Rule 3: [영수일] 보정
            UPDATE T_TEMP_RPA_DBG_PROCESSED 
            SET COLUMN_01 = COLUMN_02, 
                COLUMN_31 = COLUMN_02 
            WHERE (COLUMN_01 IS NULL OR TRIM(COLUMN_01) = '') 
              AND LEFT(REPLACE(REPLACE(COLUMN_02, '-', ''), '.', ''), 6) = v_target_ym;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_DBG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_DBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;

    END IF;

END