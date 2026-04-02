CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_KBG`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'KBG';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    -- 1. Hardcoded Column Mapping for KB Insurance (KBG)
    -- 1. Hardcoded Column Mapping for KB Insurance (KBG)
    -- 1. Hardcoded Column Mapping for KB Insurance (KBG)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        -- Mapping for LTR (Columns 01-28 + Target-only 29, 30, 31)
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
                'COLUMN_07, ', -- -
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 회계일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- -
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- -, 증권번호, 회계일
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- 보험시기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- -
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- -, -, 보험시기
            
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
                'COLUMN_17, ', -- 납입주기
                'COLUMN_18, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- 납입주기
                'COLUMN_18, '); -- -, 납입주기, -
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- 보험료
                'COLUMN_21, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- 보험료
                'COLUMN_21, '); -- -, 보험료, -
            
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
                'COLUMN_27, '); -- 상태
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- -
                'COLUMN_26, ', -- -
                'COLUMN_27, '); -- -, -, 상태
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- -
                'NULL, ', -- 납기구분(Target)
                'NULL, '); -- 납입월(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- -
                'COLUMN_29, ', -- 납기구분(Target)
                'COLUMN_30, '); -- -, 납기구분, 납입월
            
            -- 31
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL'); -- 납입일(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31'); -- 납입일(Target)

        -- Mapping for CAR (Columns 01-25 + Target-only 26, 27, 28)
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
                'COLUMN_10, ', -- 회계일
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 회계일
                'COLUMN_11, ', -- -
                'COLUMN_12, '); -- 회계일, -, -
            
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
                'COLUMN_18, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- -
                'COLUMN_18, '); -- -, -, -
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 보험료
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- 구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 보험료
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- 보험료, -, 구분
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- -
                'COLUMN_23, ', -- -
                'COLUMN_24, '); -- 보험시작일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- -
                'COLUMN_23, ', -- -
                'COLUMN_24, '); -- -, -, 보험시작일
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- -
                'NULL, ', -- 납기구분(Target)
                'NULL, '); -- 납입월(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- -
                'COLUMN_26, ', -- 납기구분(Target)
                'COLUMN_27, '); -- -, 납기구분, 납입월
            
            -- 28
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL'); -- 납입일(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28'); -- 납입일(Target)

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
                'COLUMN_07, ', -- -
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 회계일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- -
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- -, 증권번호, 회계일
            
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
                'COLUMN_14, ', -- 건수
                'COLUMN_15, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- -
                'COLUMN_14, ', -- 건수
                'COLUMN_15, '); -- -, 건수, -
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- 납입주기
                'COLUMN_18, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- -
                'COLUMN_17, ', -- 납입주기
                'COLUMN_18, '); -- -, 납입주기, -
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- -
                'NULL, '); -- 납기구분(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- -
                'COLUMN_20, ', -- -
                'COLUMN_21, '); -- -, -, 납기구분(Target)
            
            -- 22-23
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납입월(Target)
                'NULL'); -- 납입일(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 납입월(Target)
                'COLUMN_23'); -- 납입월(Target), 납입일(Target)
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
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_KBG_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_KBG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''KBG'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''KBG'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_08 <> ''증권번호'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        -- [LTR Logic]
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납기구분 = 년납, 납입월 = 해당월, 납입일 = 회계일(09)
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_29 = '년납', COLUMN_30 = v_target_ym, COLUMN_31 = COLUMN_09;

            -- Rule 2: [보험시기](12) check
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE LEFT(REPLACE(REPLACE(COLUMN_12, '-', ''), '.', ''), 6) <> v_target_ym;

            -- Rule 3: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_27='취소') > 0 AND SUM(COLUMN_27='정상') > 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_27 = '취소';
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN (SELECT COLUMN_08, MIN(SYS_ID) as mid FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08) as k ON t.COLUMN_08 = k.COLUMN_08 WHERE t.COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND t.SYS_ID <> k.mid;

            DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
            CREATE TEMPORARY TABLE tmp_agg_data SELECT COLUMN_08, SUM(CAST(REPLACE(IFNULL(COLUMN_20,'0'),',','') AS DECIMAL(18,0))) as s20, SUM(CAST(REPLACE(IFNULL(COLUMN_22,'0'),',','') AS DECIMAL(18,0))) as s22, MIN(SYS_ID) as mid FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_10 = 'KB 금쪽같은 자녀보험' GROUP BY COLUMN_08 HAVING COUNT(*)>1;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.SYS_ID = a.mid SET t.COLUMN_20 = CAST(a.s20 AS CHAR), t.COLUMN_22 = CAST(a.s22 AS CHAR);
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.COLUMN_08 = a.COLUMN_08 WHERE t.SYS_ID <> a.mid;
            
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_19 = '0';

            -- Rule 4: 보험료 마이너스 -> 플러스
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_20 = CAST(ABS(CAST(REPLACE(COLUMN_20,',','') AS DECIMAL(18,0))) AS CHAR) WHERE COLUMN_20 LIKE '-%';
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_22 = CAST(ABS(CAST(REPLACE(COLUMN_22,',','') AS DECIMAL(18,0))) AS CHAR) WHERE COLUMN_22 LIKE '-%';

        -- [CAR Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납기구분 = 년납, 납입월 = 해당월, 납입일 = 회계일(10)
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_26 = '년납', COLUMN_27 = v_target_ym, COLUMN_28 = COLUMN_10;

            -- Rule 2: [구분](21) = "환추징" 삭제
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_21 = '환추징';

            -- Rule 3: [보험시작일](24) check
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE LEFT(REPLACE(COLUMN_24, '-', ''), 6) < v_target_ym;

            -- Rule 4: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_21='취소') > 0 AND SUM(COLUMN_21='정상') > 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_21 = '취소';
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND (COLUMN_19 LIKE '-%' OR CAST(REPLACE(COLUMN_19,',','') AS DECIMAL(18,0)) < 0);

            DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
            CREATE TEMPORARY TABLE tmp_agg_data SELECT COLUMN_08, SUM(CAST(REPLACE(IFNULL(COLUMN_19,'0'),',','') AS DECIMAL(18,0))) as s19, MIN(SYS_ID) as mid FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_12 = '공동' GROUP BY COLUMN_08 HAVING COUNT(*)>1;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.SYS_ID = a.mid SET t.COLUMN_19 = CAST(a.s19 AS CHAR);
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.COLUMN_08 = a.COLUMN_08 WHERE t.SYS_ID <> a.mid;

            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t
            INNER JOIN (SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING COUNT(*) > 1 AND SUM(COLUMN_21 <> '정상') = 0) as d ON t.COLUMN_08 = d.COLUMN_08
            WHERE t.COLUMN_18 = '0';

        -- [GEN Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납기구분 = 년납, 납입월 = 해당월, 납입일 = 회계일(09)
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_21 = '년납', COLUMN_22 = v_target_ym, COLUMN_23 = COLUMN_09;

            -- Rule 2: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_16='취소') > 0 AND SUM(COLUMN_16='정상') > 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_16 = '취소';
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND (COLUMN_15 LIKE '-%' OR CAST(REPLACE(COLUMN_15,',','') AS DECIMAL(18,0)) < 0);

            -- Rule 3: [건수](14) = "0" 삭제
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_14 = '0' OR CAST(COLUMN_14 AS SIGNED) = 0;

            -- Rule 4: Combined filtering
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED 
            WHERE (COLUMN_14 = '1' OR CAST(COLUMN_14 AS SIGNED) = 1)
              AND COLUMN_17 NOT IN ('월납', '일시납')
              AND LEFT(REPLACE(REPLACE(COLUMN_19, '-', ''), '.', ''), 6) <> v_target_ym;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_KBG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;

    END IF;

END