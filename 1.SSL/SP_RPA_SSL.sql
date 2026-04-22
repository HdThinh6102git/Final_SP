CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_SSL`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'SSL';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSL_PROCESSED;
    END;

    -- Table Mapping by Insurance Type
    IF UPPER(IN_INSURANCE_TYPE) = 'LIF' THEN
        SET v_raw_table = 'T_RPA_LIFE_RAW';
        SET v_proc_table = 'T_RPA_LIFE_PROCESSED';
    END IF;

    -- 1. Hardcoded Column Mapping for Samsung Life (SSL)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-28 + Target-only 29)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 모집년월
            'COLUMN_02, ', -- 대리점코드
            'COLUMN_03, '); -- 대리점명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 모집년월
            'COLUMN_02, ', -- 대리점코드
            'COLUMN_03, '); -- 대리점명
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 수금지사코드
            'COLUMN_05, ', -- 수금지사명
            'COLUMN_06, '); -- 수금설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 수금지사코드
            'COLUMN_05, ', -- 수금지사명
            'COLUMN_06, '); -- 수금설계사코드
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 수금설계사명
            'COLUMN_08, ', -- 모집설계사코드
            'COLUMN_09, '); -- 모집설계사명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 수금설계사명
            'COLUMN_08, ', -- 모집설계사코드
            'COLUMN_09, '); -- 모집설계사명
        
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
            'COLUMN_15, '); -- 최종납입년월
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
            'COLUMN_19, ', -- 납입개월수
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 납입개월수
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입방법
        
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
            'COLUMN_26, ', -- 유지년월
            'COLUMN_27, '); -- 유지일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 종납횟수
            'COLUMN_26, ', -- 유지년월
            'COLUMN_27, '); -- 유지일자
        
        -- 28-29
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 보종코드(상품코드)
            'NULL'); -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 보종코드(상품코드)
            'COLUMN_29'); -- 납기구분
    
    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-28)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- 모집년월
            'COLUMN_02, ', -- 대리점코드
            'COLUMN_03, '); -- 대리점명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- 모집년월
            'COLUMN_02, ', -- 대리점코드
            'COLUMN_03, '); -- 대리점명
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 수금지사코드
            'COLUMN_05, ', -- 수금지사명
            'COLUMN_06, '); -- 수금설계사코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 수금지사코드
            'COLUMN_05, ', -- 수금지사명
            'COLUMN_06, '); -- 수금설계사코드
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 수금설계사명
            'COLUMN_08, ', -- 모집설계사코드
            'COLUMN_09, '); -- 모집설계사명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 수금설계사명
            'COLUMN_08, ', -- 모집설계사코드
            'COLUMN_09, '); -- 모집설계사명
        
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
            'COLUMN_15, '); -- 최종납입년월
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
            'COLUMN_19, ', -- 납입개월수
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 납입개월수
            'COLUMN_20, ', -- 납입주기
            'COLUMN_21, '); -- 납입방법
        
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
            'COLUMN_26, ', -- 유지년월
            'COLUMN_27, '); -- 유지일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 종납횟수
            'COLUMN_26, ', -- 유지년월
            'COLUMN_27, '); -- 유지일자
        
        -- 28
        SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_28'); -- 보종코드(상품코드)
        SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_28'); -- 보종코드(상품코드)
    END IF;

    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSL_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_SSL_PROCESSED LIKE T_RPA_LIFE_PROCESSED');
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_SSL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM T_RPA_LIFE_RAW ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND CONTRACT_TYPE = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        -- [NEW Logic]
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 맨 마지막열 값 추가(1개)
            -- ① 항목명 : 납기구분 / 항목값 : 년납
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_SSL_PROCESSED SET COLUMN_29 = '년납';
        END IF;

        -- [EXT Logic]
        IF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
            /*
                1. 증권번호 편집
                ① 증번 숫자형식으로 변경 후 글자수 체크(len함수)
                → 글자수가 “14&10”이면 증번앞에 “000”을 붙여서 만들어 줌
                → 글자수가 “12”이면 증번앞에 “0”을 붙여서 만들어 줌
                → 글자수가 “13”이면 증번 그대로 사용
            */
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_10 = CASE
                WHEN CHAR_LENGTH(REGEXP_REPLACE(COLUMN_10, '[^0-9]', '')) IN (10, 14)
                    THEN CONCAT('000', REGEXP_REPLACE(COLUMN_10, '[^0-9]', ''))
                WHEN CHAR_LENGTH(REGEXP_REPLACE(COLUMN_10, '[^0-9]', '')) = 12
                    THEN CONCAT('0', REGEXP_REPLACE(COLUMN_10, '[^0-9]', ''))
                ELSE REGEXP_REPLACE(COLUMN_10, '[^0-9]', '')
            END;

            /*
                2. 종납일자 “0000-00-00” 추출 후 
                → [보험계약상태]=“반송/철회”면, [종납일자]에 [계약일자]로 수정
            */
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_24 = COLUMN_14
            WHERE COLUMN_24 = '0000-00-00'
              AND (COLUMN_22 LIKE '%반송%' OR COLUMN_22 LIKE '%철회%');

            /*
                3. 최종납입년월 편집
                ① [최종납입년월] “빈셀” 추출 후 
                → [보험계약상태]=“반송/철회”면, [최종납입년월]에 “계약년월＂로 수정
                ② [최종납입년월]이 실효해당월인데 [보험계약상태]=“정상”인 경우
                → [최종납입년월]에 [유지년월] 값으로 수정
                * 실효해당월 계산 : 최종납입년월 = 마감월도 -2월
                (예시, 마감월도 2026.01 / 최종납입년월 2025.11)
                ③ [납입주기]=“6개월/12월납”이면, [최종납입년월]에 [유지년월] 값으로 수정
            */
            -- Rule 3.1: [최종납입년월] “빈셀” 추출 후 
            --    → [보험계약상태]=“반송/철회”면, [최종납입년월]에 “계약년월＂로 수정
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_15 = DATE_FORMAT(COLUMN_14, '%Y-%m')
            WHERE (COLUMN_15 IS NULL OR COLUMN_15 = '')
              AND (COLUMN_22 LIKE '%반송%' OR COLUMN_22 LIKE '%철회%');

            -- Rule 3.2: [최종납입년월]이 실효해당월인데 [보험계약상태]=“정상”인 경우
            --    → [최종납입년월]에 [유지년월] 값으로 수정
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_15 = COLUMN_26
            WHERE COLUMN_15 = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 2 MONTH), '%Y-%m')
              AND COLUMN_22 LIKE '%정상%';

            -- Rule 3.3: [납입주기]=“6개월/12월납”이면, [최종납입년월]에 [유지년월] 값으로 수정
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_15 = COLUMN_26
            WHERE COLUMN_20 IN ('6월납', '12월납');

            -- Rule 4: [종납횟수]=“0”이면, “1”로 수정
            UPDATE T_TEMP_RPA_SSL_PROCESSED SET COLUMN_25 = '1' WHERE COLUMN_25 = '0';

            -- Rule 5: [납입주기]=“일시납”이면, [합계보험료] 값을 “0”으로 / [종납횟수] 값을 “1”로 수정
            UPDATE T_TEMP_RPA_SSL_PROCESSED SET COLUMN_16 = '0', COLUMN_25 = '1' WHERE COLUMN_20 = '일시납';

            -- Rule 6: [상품명]=“기업복지 또는 단체보장” & [보험계약상태]="해지"면, [소멸년월]YYYYMM과 [최종납입년월]YYYYMM을 비교(소멸년월>최종납입년월) → FALSE 데이터의 [최종납입년월]을 [소멸월도]로 값수정
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_15 = LEFT(COLUMN_23, 7)
            WHERE (COLUMN_13 LIKE '%기업복지%' OR COLUMN_13 LIKE '%단체보장%')
              AND COLUMN_22 = '해지'
              AND DATE_FORMAT(COLUMN_23, '%Y%m') <= REPLACE(COLUMN_15, '-', '');

            -- Rule 7: [보험계약상태]=“실효” & [최종납입월]=“실효 3년 경과”면, [보험계약상태상태]값을 “시효”로 변경
            -- 3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하
            UPDATE T_TEMP_RPA_SSL_PROCESSED
            SET COLUMN_22 = '시효'
            WHERE COLUMN_22 = '실효'
              AND PERIOD_DIFF(DATE_FORMAT(CURDATE(), '%Y%m'), REPLACE(COLUMN_15, '-', '')) >= 38;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_SSL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSL_PROCESSED;

    END IF;

END