CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_HKF`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'HKF';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HKF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_sorted_hkf;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        -- Mapping for LTR (Columns 01-30 + Target-only 31, 32)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 순번, 영수일, 계약일자
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 계약번호
                'COLUMN_05, ', -- 상품코드
                'COLUMN_06, '); -- 상품명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 계약번호
                'COLUMN_05, ', -- 상품코드
                'COLUMN_06, '); -- 계약번호, 상품코드, 상품명
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 계약회차
                'COLUMN_08, ', -- 수수료회차
                'COLUMN_09, '); -- 계약자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 계약회차
                'COLUMN_08, ', -- 수수료회차
                'COLUMN_09, '); -- 계약회차, 수수료회차, 계약자명
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 피보험자명, 상태, 납방
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 신계약 CSM
                'COLUMN_14, ', -- 영수보험료
                'COLUMN_15, '); -- 합계보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 신계약 CSM
                'COLUMN_14, ', -- 영수보험료
                'COLUMN_15, '); -- 신계약 CSM, 영수보험료, 합계보험료
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 보장영수P
                'COLUMN_17, ', -- 적립영수P
                'COLUMN_18, '); -- 기타영수P
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 보장영수P
                'COLUMN_17, ', -- 적립영수P
                'COLUMN_18, '); -- 보장영수P, 적립영수P, 기타영수P
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 월납환산
                'COLUMN_20, ', -- 실손수정P
                'COLUMN_21, '); -- 실손외수정P
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 월납환산
                'COLUMN_20, ', -- 실손수정P
                'COLUMN_21, '); -- 월납환산, 실손수정P, 실손외수정P
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 만기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 수정보험료, 납기, 만기
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 보장/적립
                'COLUMN_26, ', -- 태아여부
                'COLUMN_27, '); -- 사용인
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 보장/적립
                'COLUMN_26, ', -- 태아여부
                'COLUMN_27, '); -- 보장/적립, 태아여부, 사용인
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 사용인명
                'COLUMN_29, ', -- 취급자
                'COLUMN_30, '); -- 취급자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 사용인명
                'COLUMN_29, ', -- 취급자
                'COLUMN_30, '); -- 사용인명, 취급자, 취급자명

            -- 31-32
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- -
                'NULL'); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 납기구분
                'COLUMN_32'); -- 납기구분, 납입월

        -- Mapping for GEN (Columns 01-20 + Target-only 21-26)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 계약일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 순번
                'COLUMN_02, ', -- 영수일
                'COLUMN_03, '); -- 순번, 영수일, 계약일자
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 계약만료일자
                'COLUMN_05, ', -- 계약번호
                'COLUMN_06, '); -- 상품코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 계약만료일자
                'COLUMN_05, ', -- 계약번호
                'COLUMN_06, '); -- 계약만료일자, 계약번호, 상품코드
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 상품명
                'COLUMN_08, ', -- 회차
                'COLUMN_09, '); -- 계약자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 상품명
                'COLUMN_08, ', -- 회차
                'COLUMN_09, '); -- 상품명, 회차, 계약자명
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 납방
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 피보험자명
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 피보험자명, 상태, 납방
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 인수구분
                'COLUMN_14, ', -- 대상
                'COLUMN_15, '); -- 영수보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 인수구분
                'COLUMN_14, ', -- 대상
                'COLUMN_15, '); -- 인수구분, 대상, 영수보험료
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 월납환산
                'COLUMN_17, ', -- 사용인
                'COLUMN_18, '); -- 사용인명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 월납환산
                'COLUMN_17, ', -- 사용인
                'COLUMN_18, '); -- 월납환산, 사용인, 사용인명
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 취급자
                'COLUMN_20, ', -- 취급자명
                'NULL, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 취급자
                'COLUMN_20, ', -- 취급자명
                'COLUMN_21, '); -- 취급자, 취급자명, 납기구분
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- -
                'NULL, ', -- -
                'NULL, '); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 납입월
                'COLUMN_23, ', -- 납입주기
                'COLUMN_24, '); -- 납입월, 납입주기, 만기일자
            
            -- 25-26
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- -
                'NULL'); -- -
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 납기
                'COLUMN_26'); -- 납기, 보험사성적
        END IF;

    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Select Tables based on Insurance Type
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
            SET v_proc_table = 'T_RPA_LONG_TERM_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
        END IF;

        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HKF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_HKF_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_HKF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''HKF'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''HKF'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND (COLUMN_04 <> ''증권번호'' AND COLUMN_05 <> ''증권번호'');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            
            -- [LTR Logic]
            IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
                -- Rule 1: Set payment info (납입정보 설정)
                UPDATE T_TEMP_RPA_HKF_PROCESSED SET COLUMN_31 = '년납', COLUMN_32 = v_target_ym;
                
                -- Rule 2: Convert premiums to absolute values (보험료 절댓값 변환)
                UPDATE T_TEMP_RPA_HKF_PROCESSED SET COLUMN_14 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_14,'0'), ',', '') AS SIGNED)) AS CHAR) WHERE REPLACE(COLUMN_14, ',', '') REGEXP '^-[0-9]+';
                UPDATE T_TEMP_RPA_HKF_PROCESSED SET COLUMN_22 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_22,'0'), ',', '') AS SIGNED)) AS CHAR) WHERE REPLACE(COLUMN_22, ',', '') REGEXP '^-[0-9]+';
                
                -- Rule 3: Target month filter (대상 월 필터링)
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED WHERE LEFT(REPLACE(REPLACE(COLUMN_03, '-', ''), '.', ''), 6) <> v_target_ym;

                -- Rule 4: Deduplication logic (중복 처리: 정상 + 철회 -> 정상 삭제)
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;
                CREATE TEMPORARY TABLE tmp_dup_chulhoe_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_chulhoe_hkf (seq_no)
                SELECT COLUMN_04 FROM T_TEMP_RPA_HKF_PROCESSED GROUP BY COLUMN_04
                HAVING SUM(COLUMN_11 IN ('철회/인수거부', '철회', '인수거부')) > 0 AND SUM(COLUMN_11 = '정상') > 0;
                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t INNER JOIN tmp_dup_chulhoe_hkf d ON t.COLUMN_04 = d.seq_no WHERE t.COLUMN_11 = '정상';
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;
                
            -- [GEN Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                -- Rule 1: Default values setup (기본값 설정)
                UPDATE T_TEMP_RPA_HKF_PROCESSED SET COLUMN_21 = '년납', COLUMN_22 = v_target_ym, COLUMN_23 = '일시납', COLUMN_24 = '0', COLUMN_25 = '0', COLUMN_26 = '0';
                
                -- Rule 2: Duplicate handling (중복 처리)
                -- 중복 증권번호 중 [영수보험료]="0"은 데이터 행삭제하고 [영수보험료]≠"0" 데이터는 [상태]="철회"로 값수정
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
                CREATE TEMPORARY TABLE tmp_dup_gen_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_gen_hkf (seq_no) SELECT COLUMN_05 FROM T_TEMP_RPA_HKF_PROCESSED GROUP BY COLUMN_05 HAVING COUNT(*) > 1;
                
                -- Delete zero premium duplicates (보험료 0인 중복건 삭제)
                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_05 = d.seq_no WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) = 0;
                -- Update non-zero duplicates to 'Withdraw' (보험료 0이 아닌 중복건 '철회' 처리)
                UPDATE T_TEMP_RPA_HKF_PROCESSED t INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_05 = d.seq_no SET t.COLUMN_11 = '철회' WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) <> 0;
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
            END IF;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_HKF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_HKF_PROCESSED;

    END IF;

END
