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
    
    -- [DECLARE debug logs]
    DECLARE v_log_initial_raw INT DEFAULT 0;
    DECLARE v_log_temp_initial INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- [DEBUG] Log exception
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'SQL_EXCEPTION_TRIGGERED', 0, NOW());
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_KBG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
        DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_ltr;
        DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_car;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');

    -- [DEBUG] Initialize Log Table
    CREATE TABLE IF NOT EXISTS T_RPA_DEBUG_LOG (
        BATCH_ID VARCHAR(100),
        COMPANY_CODE VARCHAR(10),
        INSURANCE_TYPE VARCHAR(50),
        CONTRACT_TYPE VARCHAR(20),
        STEP_NAME VARCHAR(100),
        ROW_COUNT INT,
        LOG_TIME DATETIME
    );
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for LTR (Columns 01-28 + Target-only 29, 30, 31)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 대리점코드
                'COLUMN_02, ', -- 지사코드
                'COLUMN_03, '); -- 지사명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 대리점코드
                'COLUMN_02, ', -- 지사코드
                'COLUMN_03, '); -- 대리점코드, 지사코드, 지사명
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 사용인코드
                'COLUMN_05, ', -- 사용인명
                'COLUMN_06, '); -- 지점코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 사용인코드
                'COLUMN_05, ', -- 사용인명
                'COLUMN_06, '); -- 사용인코드, 사용인명, 지점코드
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 지점명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 회계일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 지점명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 지점명, 증권번호, 회계일
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 상품명
                'COLUMN_11, ', -- 상품코드
                'COLUMN_12, '); -- 보험시기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 상품명
                'COLUMN_11, ', -- 상품코드
                'COLUMN_12, '); -- 상품명, 상품코드, 보험시기
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 태아여부
                'COLUMN_14, ', -- 태아선지급
                'COLUMN_15, '); -- 계약자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 태아여부
                'COLUMN_14, ', -- 태아선지급
                'COLUMN_15, '); -- 태아여부, 태아선지급, 계약자
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 계약자주민번호
                'COLUMN_17, ', -- 피보험자
                'COLUMN_18, '); -- 피보험자주민번호
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 계약자주민번호
                'COLUMN_17, ', -- 피보험자
                'COLUMN_18, '); -- 계약자주민번호, 피보험자, 피보험자주민번호
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 건수
                'COLUMN_20, ', -- 보험료
                'COLUMN_21, '); -- 보장보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 건수
                'COLUMN_20, ', -- 보험료
                'COLUMN_21, '); -- 건수, 보험료, 보장보험료
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납방
                'COLUMN_24, '); -- 납기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 수정보험료
                'COLUMN_23, ', -- 납방
                'COLUMN_24, '); -- 수정보험료, 납방, 납기
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 보기
                'COLUMN_26, ', -- 군
                'COLUMN_27, '); -- 구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 보기
                'COLUMN_26, ', -- 군
                'COLUMN_27, '); -- 보기, 군, 구분
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 매출발생일자
                'NULL, ',       -- 납기구분 (Target)
                'NULL, ');      -- 납입월 (Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 매출발생일자
                'COLUMN_29, ', -- 납기구분 (Target)
                'COLUMN_30, '); -- 매출발생일자, 납기구분, 납입월
            
            -- 31
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL');        -- 납입일 (Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31');   -- 납입일 (Target)

        -- Mapping for CAR (Columns 01-25 + Target-only 26, 27, 28)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 대리점코드
                'COLUMN_02, ', -- 지사코드
                'COLUMN_03, '); -- 지사명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 대리점코드
                'COLUMN_02, ', -- 지사코드
                'COLUMN_03, '); -- 대리점코드, 지사코드, 지사명
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 사용인코드
                'COLUMN_05, ', -- 사용인명
                'COLUMN_06, '); -- 지점코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 사용인코드
                'COLUMN_05, ', -- 사용인명
                'COLUMN_06, '); -- 사용인코드, 사용인명, 지점코드
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 지점명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 등급
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 지점명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 지점명, 증권번호, 등급
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 회계일
                'COLUMN_11, ', -- 상품코드
                'COLUMN_12, '); -- 상품명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 회계일
                'COLUMN_11, ', -- 상품코드
                'COLUMN_12, '); -- 회계일, 상품코드, 상품명
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 계약자명
                'COLUMN_14, ', -- 계약자주민번호
                'COLUMN_15, '); -- 피보험자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 계약자명
                'COLUMN_14, ', -- 계약자주민번호
                'COLUMN_15, '); -- 계약자명, 계약자주민번호, 피보험자명
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 피보험자주민번호
                'COLUMN_17, ', -- 차량번호
                'COLUMN_18, '); -- 건수
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 피보험자주민번호
                'COLUMN_17, ', -- 차량번호
                'COLUMN_18, '); -- 피보험자주민번호, 차량번호, 건수
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 보험료
                'COLUMN_20, ', -- 인수유형
                'COLUMN_21, '); -- 구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 보험료
                'COLUMN_20, ', -- 인수유형
                'COLUMN_21, '); -- 보험료, 인수유형, 구분
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 스코어
                'COLUMN_23, ', -- 매출발생일자
                'COLUMN_24, '); -- 보험시작일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 스코어
                'COLUMN_23, ', -- 매출발생일자
                'COLUMN_24, '); -- 스코어, 매출발생일자, 보험시작일
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 보험종료일
                'NULL, ',       -- 납기구분 (Target)
                'NULL, ');      -- 납입월 (Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 보험종료일
                'COLUMN_26, ', -- 납기구분 (Target)
                'COLUMN_27, '); -- 보험종료일, 납기구분, 납입월
            
            -- 28
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL');        -- 납입주기 (Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28');   -- 납입주기 (Target)

        -- Mapping for GEN (Columns 01-20 + Target-only 21, 22, 23)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 대리점코드
                'COLUMN_02, ', -- 지사코드
                'COLUMN_03, '); -- 지사명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 대리점코드
                'COLUMN_02, ', -- 지사코드
                'COLUMN_03, '); -- 대리점코드, 지사코드, 지사명
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 사용인코드
                'COLUMN_05, ', -- 사용인명
                'COLUMN_06, '); -- 지점코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 사용인코드
                'COLUMN_05, ', -- 사용인명
                'COLUMN_06, '); -- 사용인코드, 사용인명, 지점코드
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 지점명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 회계일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 지점명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 지점명, 증권번호, 회계일
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 상품코드
                'COLUMN_11, ', -- 상품명
                'COLUMN_12, '); -- 계약자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 상품코드
                'COLUMN_11, ', -- 상품명
                'COLUMN_12, '); -- 상품코드, 상품명, 계약자명
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 주민번호
                'COLUMN_14, ', -- 건수
                'COLUMN_15, '); -- 보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 주민번호
                'COLUMN_14, ', -- 건수
                'COLUMN_15, '); -- 주민번호, 건수, 보험료
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 구분
                'COLUMN_17, ', -- 납방
                'COLUMN_18, '); -- 입력일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 구분
                'COLUMN_17, ', -- 납방
                'COLUMN_18, '); -- 구분, 납방, 입력일
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 보험시작일자
                'COLUMN_20, ', -- 보험종료일자
                'NULL, ');      -- 납기구분 (Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 보험시작일자
                'COLUMN_20, ', -- 보험종료일자
                'COLUMN_21, '); -- 보험시작일자, 보험종료일자, 납기구분
            
            -- 22-23
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ',       -- 납입월 (Target)
                'NULL');        -- 납입일 (Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 납입월 (Target)
                'COLUMN_23');   -- 납입월, 납입일
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

        -- [DEBUG] Capture Initial Counts
        SET @sql_raw_count = CONCAT('SELECT COUNT(*) INTO @raw_count FROM ', v_raw_table, ' WHERE COMPANY_CODE = ''KBG'' AND BATCH_ID = ''', IN_BATCH_ID, ''' AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''')');
        PREPARE stmt_rc FROM @sql_raw_count; EXECUTE stmt_rc; DEALLOCATE PREPARE stmt_rc;
        SET v_log_initial_raw = @raw_count;
        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_KBG_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 3. Apply Transformation Logic
        
        -- [LTR Logic]
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* 1. 맨 마지막열 값 추가(3개)
            ① 항목명I : 납기구분 / 항목값 : 년납
            ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            ③ 항목명III : 납입일 / 항목값 : 회계일과 동일한 값으로 반영
            */
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_29 = '년납', COLUMN_30 = v_target_ym, COLUMN_31 = COLUMN_09;

            -- [DEBUG] Trace sample value of COLUMN_12 and v_target_ym
            SELECT COLUMN_12 INTO @sample_col12 FROM T_TEMP_RPA_KBG_PROCESSED LIMIT 1;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, CONCAT('DEBUG_LTR_DATE: ', COALESCE(@sample_col12, 'NULL'), ' / Target: ', v_target_ym), 0, NOW());

            /* 2. [증권번호] 오름차순 정렬 정렬 후 [보험시기] ≠해당월이면 삭제
            */
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE LEFT(REPLACE(REPLACE(COLUMN_12, '-', ''), '.', ''), 6) <> v_target_ym;
            
            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_LTR_RULE2', v_row_count, NOW());

            /* 3. 증권번호 중복 편집
            ① 중복 증권번호의 [구분]=각각"취소,정상"이면 [구분]="취소"로 수정 및 중복데이터 행삭제
            ② 중복 증번번호 중 [상품명]="KB 금쪽같은 자녀보험"이면 [보험료],[수정보험료]를 합산하여 한건으로 값수정([건수]="0"인 데이터 행삭제)
            */
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_27='취소') > 0 AND SUM(COLUMN_27='정상') > 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_27 = '취소';
            
            -- MySQL Delete Limitation Fix: Use intermediate temp table
            DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_ltr;
            CREATE TEMPORARY TABLE tmp_del_kbg_ltr SELECT COLUMN_08, MIN(SYS_ID) as mid FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08;
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_del_kbg_ltr k ON t.COLUMN_08 = k.COLUMN_08 WHERE t.COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND t.SYS_ID <> k.mid;
            DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_ltr;

            DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
            CREATE TEMPORARY TABLE tmp_agg_data SELECT COLUMN_08, SUM(CAST(REPLACE(IFNULL(COLUMN_20,'0'),',','') AS DECIMAL(18,0))) as s20, SUM(CAST(REPLACE(IFNULL(COLUMN_22,'0'),',','') AS DECIMAL(18,0))) as s22, MIN(SYS_ID) as mid FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_10 = 'KB 금쪽같은 자녀보험' GROUP BY COLUMN_08 HAVING COUNT(*)>1;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.SYS_ID = a.mid SET t.COLUMN_20 = CAST(a.s20 AS CHAR), t.COLUMN_22 = CAST(a.s22 AS CHAR);
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.COLUMN_08 = a.COLUMN_08 WHERE t.SYS_ID <> a.mid;
            
            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_LTR_RULE3', v_row_count, NOW());
            
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_19 = '0';

            /* 4. [보험료],[수정보험료]="마이너스금액"이면 "플러스"로 변경
            */
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_20 = CAST(ABS(CAST(REPLACE(COLUMN_20,',','') AS DECIMAL(18,0))) AS CHAR) WHERE COLUMN_20 LIKE '-%';
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_22 = CAST(ABS(CAST(REPLACE(COLUMN_22,',','') AS DECIMAL(18,0))) AS CHAR) WHERE COLUMN_22 LIKE '-%';

            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_LTR_LOGIC', v_row_count, NOW());

        -- [CAR Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* 1. 맨 마지막열 값 추가(3개)
            ① 항목명I : 납기구분 / 항목값 : 년납
            ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            ③ 항목명III : 납입주기 / 항목값 : 일시납
            ※ 전체 행에 반영 
            */
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_26 = '년납', COLUMN_27 = v_target_ym, COLUMN_28 = COLUMN_10;

            /* 2. [증권번호] 오름차순 정렬 후 [구분]="환추징"이면 데이터 행삭제 */
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_21 = '환추징';

            -- [DEBUG] Trace sample value of COLUMN_24 and v_target_ym
            SELECT COLUMN_24 INTO @sample_col24 FROM T_TEMP_RPA_KBG_PROCESSED LIMIT 1;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, CONCAT('DEBUG_CAR_DATE: ', COALESCE(@sample_col24, 'NULL'), ' / Target: ', v_target_ym), 0, NOW());

            /* 3. [보험시작일]<"해당월"이면 데이터 행삭제 */
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE LEFT(REPLACE(COLUMN_24, '-', ''), 6) < v_target_ym;

            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_CAR_RULE3', v_row_count, NOW());

            /* 4. 중복 증번 편집
            ① 중복 증권번호 중 [구분]="정상,취소"이면 [구분]="취소" 값수정 및 [보험료]="마이너스금액" 데이터 행삭제
            ② 중복 증권번호 중 [상품명]="공동"이면 [보험료]="보험료 합산" 한건으로 수정 및 중복 데이터 행삭제
            ③ 중복 증권번호 중 [구분]="정상"으로 동일한 경우, [건수]="0"인 데이터 행삭제
            */
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_21='취소') > 0 AND SUM(COLUMN_21='정상') > 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_21 = '취소';
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND (COLUMN_19 LIKE '-%' OR CAST(REPLACE(COLUMN_19,',','') AS DECIMAL(18,0)) < 0);

            DROP TEMPORARY TABLE IF EXISTS tmp_agg_data;
            CREATE TEMPORARY TABLE tmp_agg_data SELECT COLUMN_08, SUM(CAST(REPLACE(IFNULL(COLUMN_19,'0'),',','') AS DECIMAL(18,0))) as s19, MIN(SYS_ID) as mid FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_12 = '공동' GROUP BY COLUMN_08 HAVING COUNT(*)>1;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.SYS_ID = a.mid SET t.COLUMN_19 = CAST(a.s19 AS CHAR);
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_agg_data a ON t.COLUMN_08 = a.COLUMN_08 WHERE t.SYS_ID <> a.mid;

            -- MySQL Delete Limitation Fix: Use intermediate temp table
            DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_car;
            CREATE TEMPORARY TABLE tmp_del_kbg_car SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING COUNT(*) > 1 AND SUM(COLUMN_21 <> '정상') = 0;
            DELETE t FROM T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_del_kbg_car d ON t.COLUMN_08 = d.COLUMN_08 WHERE t.COLUMN_18 = '0';
            DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_car;

            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_CAR_LOGIC', v_row_count, NOW());

        -- [GEN Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* 1. 맨 마지막열 값 추가(3개)
            ① 항목명I : 납기 / 항목값 : 0
            ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            ③ 항목명III : 납입일 / 항목값 : 회계일과 동일한 값으로 반영
            ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_KBG_PROCESSED SET COLUMN_21 = '년납', COLUMN_22 = v_target_ym, COLUMN_23 = COLUMN_09;

            /* 2. 중복 증권번호 중 [구분]="정상,취소"이면 [구분]="취소" 값수정 및   [보험료]="마이너스금액" 데이터 행삭제
            */
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_KBG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_16='취소') > 0 AND SUM(COLUMN_16='정상') > 0;
            UPDATE T_TEMP_RPA_KBG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_16 = '취소';
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND (COLUMN_15 LIKE '-%' OR CAST(REPLACE(COLUMN_15,',','') AS DECIMAL(18,0)) < 0);

            /* 3. [건수]="0"이면 데이터 행삭제
            */
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED WHERE COLUMN_14 = '0' OR CAST(COLUMN_14 AS SIGNED) = 0;

            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_GEN_RULE3', v_row_count, NOW());

            /* 4. [건수]="1" & [납입방법]≠"월납,일시납"이고 [보험시작일자]≠"해당월"인 데이터 행삭제
            */
            DELETE FROM T_TEMP_RPA_KBG_PROCESSED 
            WHERE (COLUMN_14 = '1' OR CAST(COLUMN_14 AS SIGNED) = 1)
              AND COLUMN_17 NOT IN ('월납', '일시납')
              AND LEFT(REPLACE(REPLACE(COLUMN_19, '-', ''), '.', ''), 6) <> v_target_ym;

            -- [DEBUG] Capture counts
            SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_GEN_LOGIC', v_row_count, NOW());
        END IF;

        -- [DEBUG] Count before final insert
        SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_KBG_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'KBG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'BEFORE_FINAL_INSERT', v_row_count, NOW());

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
        DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_ltr;
        DROP TEMPORARY TABLE IF EXISTS tmp_del_kbg_car;

    END IF;

END