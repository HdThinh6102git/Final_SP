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
    DECLARE v_log_initial_raw   INT DEFAULT 0;
    DECLARE v_log_temp_initial  INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- [DEBUG] Log exception
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'SQL_EXCEPTION_TRIGGERED', 0, NOW());
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

        -- Mapping for CAR (Columns 01-30 + Target-only 31-35)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
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

            -- 31-35 (Target-only)
            SET v_raw_cols = CONCAT(v_raw_cols,
                'NULL, ', -- 납기구분
                'NULL, ', -- 납입월
                'NULL, ', -- 납입일
                'NULL, ', -- 만기일자 (skip)
                'NULL');  -- 차량번호 (skip)
            SET v_proc_cols = CONCAT(v_proc_cols,
                'COLUMN_31, ', -- 납기구분
                'COLUMN_32, ', -- 납입월
                'COLUMN_33, ', -- 납입일
                'COLUMN_34, ', -- 만기일자
                'COLUMN_35');  -- 차량번호

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
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_table = 'T_RPA_CAR_RAW';
            SET v_proc_table = 'T_RPA_CAR_PROCESSED';    
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
        END IF;

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
        IF v_raw_table != '' THEN
            SET @sql_count_raw = CONCAT('SELECT COUNT(*) INTO @v_log_initial_raw FROM ', v_raw_table, ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' AND COMPANY_CODE = ''HKF''');
            PREPARE stmt_count FROM @sql_count_raw;
            EXECUTE stmt_count;
            DEALLOCATE PREPARE stmt_count;
            SET v_log_initial_raw = @v_log_initial_raw;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
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

        -- [DEBUG] Record TEMP_INITIAL
        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_HKF_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            
            -- [LTR Logic]
            IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
                -- Rule 1: 맨 마지막열 값 추가(2개)
                -- ① 항목명I : 납기구분 / 항목값 : 년납
                -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_31 = '년납', COLUMN_32 = v_target_ym;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE1', (SELECT COUNT(*) FROM T_TEMP_RPA_HKF_PROCESSED), NOW());

                -- Rule 2: 중복 증번 편집
                -- ① 맨아래 계 부분의 데이터 행2개 삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED
                WHERE COLUMN_01 IS NULL 
                OR COLUMN_01 = ''
                OR COLUMN_01 NOT REGEXP '^[0-9]+$';

                -- ③ 중복 계약번호 중 [상태]=각각"정상,철회/인수거부"이면 [상태]="정상" 데이터 행삭제
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;
                CREATE TEMPORARY TABLE tmp_dup_chulhoe_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_chulhoe_hkf (seq_no)
                SELECT COLUMN_04 FROM T_TEMP_RPA_HKF_PROCESSED GROUP BY COLUMN_04
                HAVING SUM(COLUMN_11 IN ('철회/인수거부', '철회', '인수거부')) > 0 
                AND SUM(COLUMN_11 = '정상') > 0;
                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t 
                INNER JOIN tmp_dup_chulhoe_hkf d ON t.COLUMN_04 = d.seq_no 
                WHERE t.COLUMN_11 = '정상';
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_chulhoe_hkf;

                -- ④ [영수보험료],[수정보험료]="마이너스 금액"이면 "플러스 금액"으로 값수정
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_14 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_14,'0'), ',', '') AS SIGNED)) AS CHAR) 
                WHERE REPLACE(COLUMN_14, ',', '') REGEXP '^-[0-9]+';
                
                UPDATE T_TEMP_RPA_HKF_PROCESSED 
                SET COLUMN_22 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_22,'0'), ',', '') AS SIGNED)) AS CHAR) 
                WHERE REPLACE(COLUMN_22, ',', '') REGEXP '^-[0-9]+';
                
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE2', (SELECT COUNT(*) FROM T_TEMP_RPA_HKF_PROCESSED), NOW());

                -- [DEBUG] Trace sample value of COLUMN_03 and v_target_ym
                SELECT COLUMN_03 INTO @sample_col03 FROM T_TEMP_RPA_HKF_PROCESSED LIMIT 1;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, CONCAT('DEBUG_LTR_DATE: ', COALESCE(@sample_col03, 'NULL'), ' / Target: ', v_target_ym), 0, NOW());

                -- Rule 3: [계약일자]≠"해당월"면 데이터 행삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED 
                WHERE LEFT(REPLACE(REPLACE(COLUMN_03, '-', ''), '.', ''), 6) <> v_target_ym;
                
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'LTR_AFTER_RULE3', (SELECT COUNT(*) FROM T_TEMP_RPA_HKF_PROCESSED), NOW());

                -- Rule 4: [납기]="세납"인 경우 → SKIP (수동처리)
                
            -- [CAR Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN

                /* Rule 1: 맨 마지막열 값 추가
                ① 납기구분 = 년납
                ② 납입월 = 해당월
                ③ 납입일 = 영수일 (COLUMN_02)
                ④ 만기일자 → SKIP (수동처리)
                ⑤ 차량번호 → SKIP (수동처리)
                */
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET COLUMN_31 = '년납',
                    COLUMN_32 = v_target_ym,
                    COLUMN_33 = COLUMN_02;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_AFTER_RULE1', (SELECT COUNT(*) FROM T_TEMP_RPA_HKF_PROCESSED), NOW());

                /* Rule 2①: 맨아래 계 부분 데이터 행2개 삭제  */
                -- ① 맨아래 계 부분의 데이터 행2개 삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED
                WHERE COLUMN_01 IS NULL 
                OR COLUMN_01 = ''
                OR COLUMN_01 NOT REGEXP '^[0-9]+$';

                /* Rule 2②: 증권번호 오름차순 정렬 */
                SET @seq := 0;
                UPDATE T_TEMP_RPA_HKF_PROCESSED
                SET SORT_ORDER_NO = (@seq := @seq + 1)
                ORDER BY COLUMN_04 ASC;

                /* Rule 2③: 중복 증권번호 중 합계보험료=0 삭제, ≠0 → 상태=철회 */
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
                CREATE TEMPORARY TABLE tmp_dup_gen_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_gen_hkf (seq_no)
                SELECT COLUMN_04 FROM T_TEMP_RPA_HKF_PROCESSED
                GROUP BY COLUMN_04 HAVING COUNT(*) > 1;

                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_04 = d.seq_no
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) = 0;

                UPDATE T_TEMP_RPA_HKF_PROCESSED t
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_04 = d.seq_no
                SET t.COLUMN_11 = '철회'
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) <> 0;

                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;

                /* Rule 2④: 차량번호, 만기일자 → SKIP (수동처리) */

                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_AFTER_RULE2', (SELECT COUNT(*) FROM T_TEMP_RPA_HKF_PROCESSED), NOW());

            -- [GEN Logic]
            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                -- Rule 2①: 맨아래 계 부분 데이터 행2개 삭제
                -- ① 맨아래 계 부분의 데이터 행2개 삭제
                DELETE FROM T_TEMP_RPA_HKF_PROCESSED
                WHERE COLUMN_01 IS NULL 
                OR COLUMN_01 = ''
                OR COLUMN_01 NOT REGEXP '^[0-9]+$';
                
                -- Rule 2③: 중복 증권번호 처리
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
                CREATE TEMPORARY TABLE tmp_dup_gen_hkf (seq_no VARCHAR(150));
                INSERT INTO tmp_dup_gen_hkf (seq_no) 
                SELECT COLUMN_05 FROM T_TEMP_RPA_HKF_PROCESSED 
                GROUP BY COLUMN_05 HAVING COUNT(*) > 1;

                DELETE t FROM T_TEMP_RPA_HKF_PROCESSED t 
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_05 = d.seq_no 
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) = 0;

                UPDATE T_TEMP_RPA_HKF_PROCESSED t 
                INNER JOIN tmp_dup_gen_hkf d ON t.COLUMN_05 = d.seq_no 
                SET t.COLUMN_11 = '철회' 
                WHERE CAST(REPLACE(IFNULL(t.COLUMN_15,'0'), ',', '') AS DECIMAL(20,2)) <> 0;

                DROP TEMPORARY TABLE IF EXISTS tmp_dup_gen_hkf;
            END IF;
        END IF;

        -- [DEBUG] Record final state
        SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_HKF_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'HKF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'BEFORE_FINAL_INSERT', v_row_count, NOW());

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
