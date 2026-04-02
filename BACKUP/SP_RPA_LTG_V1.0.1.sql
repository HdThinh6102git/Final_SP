CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_LTG`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'LTG';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- 시효 기준: 마감월 대비 38개월 이전
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Hardcoded Column Mapping for Lotte Insurance (LTG)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        -- Mapping for NEW LTR/GEN (Columns 01-109 + Target-only 110)
        IF UPPER(IN_INSURANCE_TYPE) IN ('LTR', 'GEN') THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- Columns 01-109 (Using loops/concatenation for brevity)
            SET @i = 1;
            WHILE @i <= 109 DO
                SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_', LPAD(@i, 2, '0'), ', ');
                SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_', LPAD(@i, 2, '0'), ', ');
                SET @i = @i + 1;
            END WHILE;

            -- Column 110 (Target specific)
            SET v_raw_cols = CONCAT(v_raw_cols, 'NULL');
            SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_110');

        -- CAR is skipped
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
        END IF;

    ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') THEN
        
        -- Mapping for EXT (Columns 01-34)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            SET @i = 1;
            WHILE @i <= 34 DO
                SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_', LPAD(@i, 2, '0'), IF(@i < 34, ', ', ''));
                SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_', LPAD(@i, 2, '0'), IF(@i < 34, ', ', ''));
                SET @i = @i + 1;
            END WHILE;
        END IF;

    END IF;

    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Select Tables
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
            SET v_proc_table = 'T_RPA_LONG_TERM_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_table = 'T_RPA_CAR_RAW';
            SET v_proc_table = 'T_RPA_CAR_PROCESSED';
        END IF;

        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_LTG_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_LTG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''LTG'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''LTG'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_08 <> ''증권번호'' AND COLUMN_01 <> ''증권번호'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납기구분 = 년납
            UPDATE T_TEMP_RPA_LTG_PROCESSED SET COLUMN_110 = '년납';

            -- Rule 2: [처리구분](23) ≠ "신규, 추징" 삭제
            DELETE FROM T_TEMP_RPA_LTG_PROCESSED WHERE COLUMN_23 NOT IN ('신규', '추징', '신규/추징');

            -- Rule 3: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_LTG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_23 IN ('취소', '철회', '취소/철회')) > 0 AND SUM(COLUMN_23='정상') > 0;
            UPDATE T_TEMP_RPA_LTG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_23 = '취소' WHERE t.COLUMN_23 = '정상';
            DELETE FROM T_TEMP_RPA_LTG_PROCESSED WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND (COLUMN_30 LIKE '-%' OR CAST(REPLACE(COLUMN_30,',','') AS DECIMAL(18,0)) < 0);

        ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') THEN
            -- Rule 1: [상태](11)="정상,불능" -> [실적기준일](12)="0000-00-00"
            UPDATE T_TEMP_RPA_LTG_PROCESSED SET COLUMN_12 = '0000-00-00' WHERE COLUMN_11 IN ('정상', '불능');

            -- Rule 2: 연체 처리
            UPDATE T_TEMP_RPA_LTG_PROCESSED
            SET COLUMN_11 = '연체'
            WHERE COLUMN_11 = '정상'
              AND COLUMN_34 NOT LIKE '%납입면제%' AND COLUMN_34 NOT LIKE '%완납%'
              AND COLUMN_05 < v_target_ym;

            -- Rule 3: 시효 처리
            UPDATE T_TEMP_RPA_LTG_PROCESSED SET COLUMN_11 = '시효' WHERE COLUMN_11 = '실효' AND COLUMN_05 <= v_cutoff_ym;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_LTG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;

    END IF;

END