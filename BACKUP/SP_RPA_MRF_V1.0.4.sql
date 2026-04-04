CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_MRF`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'MRF';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';
    
    -- [DECLARE debug variables]
    DECLARE v_log_initial_raw    INT          DEFAULT 0;
    DECLARE v_log_temp_initial   INT          DEFAULT 0;
    DECLARE v_log_after_rule2    INT          DEFAULT 0;
    DECLARE v_log_after_rule3    INT          DEFAULT 0;
    DECLARE v_log_after_rule4    INT          DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'SQL_EXCEPTION_TRIGGERED', 0, NOW());
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
    END;

    -- [SET internal logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Standard 38-month cutoff for Statute of Limitations (Rule 6 Existing)
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- Table Mapping by Insurance Type
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

    -- [INIT Debug Log Table]
    CREATE TABLE IF NOT EXISTS T_RPA_DEBUG_LOG (
        BATCH_ID VARCHAR(100),
        COMPANY_CODE VARCHAR(10),
        INSURANCE_TYPE VARCHAR(50),
        CONTRACT_TYPE VARCHAR(20),
        STEP_NAME VARCHAR(100),
        ROW_COUNT INT,
        LOG_TIME DATETIME
    );

    -- 1. Hardcoded Column Mapping for Meritz Fire (MRF)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-33 + Target-only 34-35 for LTR)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 일자, 증권번호, 보험료
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 납입주기, 횟수, 수금방법
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 영수, 선납, 초회수정P
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 초년도수정P, 출생후보험료, 출생후수정P
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수정P, 계약자, 수수료분급
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 신규구분, 차량번호, 입력일
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 리스크등급, 대리점설계사코드, 대리점설계사명
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 상품코드
            'COLUMN_24, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 상품코드
            'COLUMN_24, '); -- 지사명, 상품코드, 상품명
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 고위험물건
            'COLUMN_26, ', -- 공동물건
            'COLUMN_27, '); -- 행복나눔특약
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 고위험물건
            'COLUMN_26, ', -- 공동물건
            'COLUMN_27, '); -- 고위험물건, 공동물건, 행복나눔특약
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 청약일
            'COLUMN_29, ', -- 납입기간
            'COLUMN_30, '); -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 청약일
            'COLUMN_29, ', -- 납입기간
            'COLUMN_30, '); -- 청약일, 납입기간, 납입월
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 보험종료일자
            'COLUMN_32, ', -- 인수형태
            'COLUMN_33'); -- 피보험자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 보험종료일자
            'COLUMN_32, ', -- 인수형태
            'COLUMN_33'); -- 보험종료일자, 인수형태, 피보험자

        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            -- 34-35 (Target-only)
            SET v_raw_cols = CONCAT(v_raw_cols, 
                ', NULL, ', -- 납기구분(Target)
                'NULL'); -- 납입월(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                ', COLUMN_34, ', -- 납기구분(Target)
                'COLUMN_35'); -- 납기구분(Target), 납입월(Target)
        END IF;

    ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') AND UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        -- Mapping for EXT contacts (Columns 01-15 + 51-56 for LTR)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 업무시스템코드
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 단위상품코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 업무시스템코드
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 업무시스템코드, 증권번호, 단위상품코드
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 청약일자
            'COLUMN_05, ', -- 계약상태코드
            'COLUMN_06, '); -- 보험개시일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 청약일자
            'COLUMN_05, ', -- 계약상태코드
            'COLUMN_06, '); -- 청약일자, 계약상태코드, 보험개시일자
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 보험종료일자
            'COLUMN_08, ', -- 최종납입년월
            'COLUMN_09, '); -- 최종납입회차
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 보험종료일자
            'COLUMN_08, ', -- 최종납입년월
            'COLUMN_09, '); -- 보험종료일자, 최종납입년월, 최종납입회차
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 영업보험료금액
            'COLUMN_11, ', -- 총납입보험료금액
            'COLUMN_12, '); -- 취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 영업보험료금액
            'COLUMN_11, ', -- 총납입보험료금액
            'COLUMN_12, '); -- 영업보험료금액, 총납입보험료금액, 취급자코드
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 취급자명
            'COLUMN_14, ', -- 대리점설계사코드
            'COLUMN_15, '); -- 대리점설계사명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 취급자명
            'COLUMN_14, ', -- 대리점설계사코드
            'COLUMN_15, '); -- 취급자명, 대리점설계사코드, 대리점설계사명
        
        -- 51-53
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_51, ', -- 피보험자명
            'COLUMN_52, ', -- 피보험자생년월일
            'COLUMN_53, '); -- 계약상태명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_51, ', -- 피보험자명
            'COLUMN_52, ', -- 피보험자생년월일
            'COLUMN_53, '); -- 피보험자명, 피보험자생년월일, 계약상태명
        
        -- 54-56
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_54, ', -- 소멸실효일자
            'COLUMN_55, ', -- 최종납입일자
            'COLUMN_56'); -- 계약상태상세명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_54, ', -- 소멸실효일자
            'COLUMN_55, ', -- 최종납입일자
            'COLUMN_56'); -- 소멸실효일자, 최종납입일자, 계약상태상세명
    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' AND v_proc_table != '' THEN
        
        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_MRF_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;
        
        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_MRF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''MRF'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''MRF'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- [DEBUG] Capture Initial Raw and Temp counts
        SET @sql_raw_count = CONCAT('SELECT COUNT(*) INTO @v_raw_count FROM ', v_raw_table, ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') AND COMPANY_CODE = ''MRF''');
        PREPARE stmt_raw FROM @sql_raw_count;
        EXECUTE stmt_raw;
        DEALLOCATE PREPARE stmt_raw;
        SET v_log_initial_raw = @v_raw_count;
        
        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_MRF_PROCESSED;
        
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- [DEBUG] Entry into NEW block
            SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'ENTER_NEW_BLOCK', v_log_temp_initial, NOW());
            
            -- Long Term Logic
            IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
                -- Rule 1: 맨 마지막열 값 추가(2개)
                -- ① 항목명I : 납기구분 / 항목값 : 년납
                -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                -- ※ 전체 행에 반영
                UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_34 = '년납', COLUMN_35 = v_target_ym;
                
                -- Rule 2: [증권번호] 오름차순 정렬 후 [영수]≠"신계약, 취소"면 데이터 행삭제
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_07 NOT IN ('신계약', '취소');

                -- Rule 3: [일자]≠해당월 & [상품명]≠실손이면 데이터 행삭제
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED 
                WHERE LEFT(REPLACE(COLUMN_01, '-', ''), 6) <> v_target_ym AND COLUMN_24 NOT LIKE '%실손%';

                -- Rule 4: 증권번호 중복 편집
                -- 중복 증권번호 중 [영수값]="신계약,취소"면 [영수값]="취소"로 수정 및 [보험료]="마이너스금액" 데이터 행삭제
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
                CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING SUM(COLUMN_07='신계약')>0 AND SUM(COLUMN_07='취소')>0;
                UPDATE T_TEMP_RPA_MRF_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_02 = d.COLUMN_02 SET t.COLUMN_07 = '취소';
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_dup_case) AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);
            
                -- [DEBUG] Capture LTR Rule 4 count
                SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_MRF_PROCESSED;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_LTR_RULE4', v_log_after_rule4, NOW());

            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
                -- [DEBUG] Entry into CAR block
                SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'ENTER_CAR_BLOCK', v_log_after_rule2, NOW());

                -- Rule 1: 맨 마지막열 값 추가(2개)
                -- ① 항목명I : 납기구분 / 항목값 : 년납
                -- ③ 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                -- ※ 전체 행에 반영
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_BEFORE_RULE1_UPDATE', v_log_after_rule2, NOW());
                UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_34 = '년납', COLUMN_35 = v_target_ym;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_AFTER_RULE1_UPDATE', v_log_after_rule2, NOW());

                -- Rule 2: 증권번호 중복 편집
                -- ① 계약번호 오름차순 정렬
                -- ② 중복 계약번호 중 [영수]=모두 "배서"면 해당 데이터들 행삭제
                -- ③ 중복 계약번호 중 [영수]=각각"신계약,배서"면 "배서" 데이터 행삭제
                -- ④ 중복 계약번호 중 [영수]=각각"신계약,취소"면 [영수]="취소"로 수정 및 [보험료]="마이너스금액" 데이터 행삭제
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_BEFORE_RULE2_DELETE1', 31, NOW());
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING COUNT(*)>1 AND SUM(COLUMN_07<>'배서')=0) AS t);
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'CAR_BEFORE_RULE2_DELETE2', 31, NOW());
                DELETE t FROM T_TEMP_RPA_MRF_PROCESSED t WHERE COLUMN_07 = '배서' AND COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_07='신계약') AS t);
                
                -- [DEBUG] Capture CAR Rule 2 (Deletes 1-2) count
                SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_CAR_RULE2_DELETES', v_log_after_rule2, NOW());

                DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
                CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING SUM(COLUMN_07='신계약')>0 AND SUM(COLUMN_07='취소')>0;
                UPDATE T_TEMP_RPA_MRF_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_02 = d.COLUMN_02 SET t.COLUMN_07 = '취소';
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_dup_case) AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);

                -- Rule 3: [보험료]="마이너스"이면 "플러스"값으로 수정
                UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_03 = CAST(ABS(CAST(REPLACE(COLUMN_03, ',', '') AS SIGNED)) AS CHAR) WHERE COLUMN_03 LIKE '-%';

                -- [DEBUG] Capture CAR Rule counts
                SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_MRF_PROCESSED;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_CAR_LOGIC', v_log_after_rule3, NOW());

            ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
                -- Rule 1: 맨 마지막열 값 추가(2개)
                -- ① 항목명I : 납기구분 / 항목값 : 년납
                -- ③ 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                -- ※ 전체 행에 반영
                UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_34 = '년납', COLUMN_35 = v_target_ym;

                -- Rule 2: 증권번호 중복 편집
                -- ① 계약번호 오름차순 정렬
                -- ② 중복 계약번호의 [영수]=모두 "정상"면 해당 데이터들 행삭제
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING COUNT(*)>1 AND SUM(COLUMN_07<>'정상')=0) AS t);
                -- ③ 중복 계약번호 중 [계약상태]=각각"신계약,배서"면 "배서" 데이터 행삭제
                DELETE t FROM T_TEMP_RPA_MRF_PROCESSED t WHERE COLUMN_07 = '배서' AND COLUMN_02 IN (SELECT * FROM (SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_07='신계약') AS t);
                
                -- [DEBUG] Capture GEN Rule 2 (Deletes 1-2) count
                SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_GEN_RULE2_DELETES', v_log_after_rule2, NOW());
                -- ④ 중복 계약번호 중 [계약상태]=각각"신계약,취소"면 [영수]="취소"로 수정 및 [보험료]="마이너스금액" 데이터 행삭제
                DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
                CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_02 FROM T_TEMP_RPA_MRF_PROCESSED GROUP BY COLUMN_02 HAVING SUM(COLUMN_07='신계약')>0 AND SUM(COLUMN_07='취소')>0;
                UPDATE T_TEMP_RPA_MRF_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_02 = d.COLUMN_02 SET t.COLUMN_07 = '취소';
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_dup_case) AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);
                -- ⑤ 청약일≠해당월 & [보험료]="마이너스금액" 데이터 행삭제
                DELETE FROM T_TEMP_RPA_MRF_PROCESSED WHERE LEFT(REPLACE(COLUMN_28, '-', ''), 6) <> v_target_ym AND (COLUMN_03 LIKE '-%' OR CAST(REPLACE(COLUMN_03,',','') AS SIGNED) < 0);

                -- [DEBUG] Capture GEN Final logic count
                SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_MRF_PROCESSED;
                INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_GEN_LOGIC', v_log_after_rule3, NOW());
            END IF;

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' AND UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            -- Rule 1: [계약상태명]=“정상,해지,해지불능”이면 [소멸실효일자]를 “0000-00-00”으로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_54 = '0000-00-00' WHERE COLUMN_56 IN ('정상', '해지', '해지불능');

            -- Rule 2: [계약상세상태명]=“모든 완납, 모든 납입면제 제외” 후 [계약상태명]="정상"건만 추출하여 [최종납입년월] 연체건은 [계약상태명]값을 "연체"로 값수정
            -- 연체기준 : 최종납입월도가 마감월도보다 작은 경우(예시, 최종납입월도 2025.12 / 마감월도 2026.01 → 연체)

            UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_53 = '연체'
            WHERE COLUMN_56 NOT LIKE '%완납%' AND COLUMN_56 NOT LIKE '%납입면제%' AND COLUMN_53 = '정상'
              AND DATE_FORMAT(STR_TO_DATE(COLUMN_08, '%c/%e/%Y'), '%Y%m') < v_target_ym;

            -- Rule 3: [계약상세상태명]=“중지＂이면, [계약상태명]값을 "정상"으로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_53 = '정상' WHERE COLUMN_56 = '중지';

            -- Rule 4: [청약일자],[보험개시일자],[보험종료일자]를 간단한날짜 서식으로 변경
            UPDATE T_TEMP_RPA_MRF_PROCESSED SET 
                COLUMN_04 = DATE_FORMAT(STR_TO_DATE(COLUMN_04, '%c/%e/%Y'), '%Y-%m-%d'),
                COLUMN_06 = DATE_FORMAT(STR_TO_DATE(COLUMN_06, '%c/%e/%Y'), '%Y-%m-%d'),
                COLUMN_07 = DATE_FORMAT(STR_TO_DATE(COLUMN_07, '%c/%e/%Y'), '%Y-%m-%d');

            -- Rule 5: [계약상세상태명]=“취소,철회”이면, [최종납입일자]="계약일자"로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_55 = COLUMN_04 WHERE COLUMN_56 IN ('취소', '철회');

            -- Rule 6: [계약상세상태명]=“실효” & [최종납입년월]=“실효 3년 경과”면, [계약상태명]값을 “시효”로 변경
            -- 3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하
            UPDATE T_TEMP_RPA_MRF_PROCESSED SET COLUMN_53 = '시효'
            WHERE COLUMN_56 = '실효' AND DATE_FORMAT(STR_TO_DATE(COLUMN_08, '%c/%e/%Y'), '%Y%m') <= v_cutoff_ym;
        END IF;

        -- [DEBUG] Count before final insert
        SELECT COUNT(*) INTO v_row_count FROM T_TEMP_RPA_MRF_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'MRF', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'BEFORE_FINAL_INSERT', v_row_count, NOW());

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_MRF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;

    END IF;

END