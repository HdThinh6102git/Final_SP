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
                'COLUMN_01, ', -- 소속기관
                'COLUMN_02, ', -- 팀기관
                'COLUMN_03, '); -- 취급자코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 소속기관
                'COLUMN_02, ', -- 팀기관
                'COLUMN_03, '); -- 소속기관, 팀기관, 취급자코드
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 취급자
                'COLUMN_05, ', -- 사용인코드
                'COLUMN_06, '); -- 사용인
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 취급자
                'COLUMN_05, ', -- 사용인코드
                'COLUMN_06, '); -- 취급자, 사용인코드, 사용인
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 증권번호
                'COLUMN_08, ', -- 상품명
                'COLUMN_09, '); -- 계약자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 증권번호
                'COLUMN_08, ', -- 상품명
                'COLUMN_09, '); -- 증권번호, 상품명, 계약자
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 피보험자
                'COLUMN_11, ', -- 계상일자
                'COLUMN_12, '); -- 보험시기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 피보험자
                'COLUMN_11, ', -- 계상일자
                'COLUMN_12, '); -- 피보험자, 계상일자, 보험시기
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 실납입기간
                'COLUMN_14, ', -- 수금방법
                'COLUMN_15, '); -- 납입주기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 실납입기간
                'COLUMN_14, ', -- 수금방법
                'COLUMN_15, '); -- 실납입기간, 수금방법, 납입주기
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 계약상태명
                'COLUMN_17, ', -- 월납환산보험료
                'COLUMN_18, '); -- 영수보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 계약상태명
                'COLUMN_17, ', -- 월납환산보험료
                'COLUMN_18, '); -- 계약상태명, 월납환산보험료, 영수보험료
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 보장보험료
                'COLUMN_20, ', -- 환산실적
                'COLUMN_21, '); -- 핸드폰
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 보장보험료
                'COLUMN_20, ', -- 환산실적
                'COLUMN_21, '); -- 보장보험료, 환산실적, 핸드폰
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 핸드폰뒷자리
                'COLUMN_23, ', -- 자택연락처
                'COLUMN_24, '); -- 자택연락처뒷자리
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 핸드폰뒷자리
                'COLUMN_23, ', -- 자택연락처
                'COLUMN_24, '); -- 핸드폰뒷자리, 자택연락처, 자택연락처뒷자리
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 증권발행여부
                'COLUMN_26, ', -- 청약형태
                'COLUMN_27, '); -- 만기구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 증권발행여부
                'COLUMN_26, ', -- 청약형태
                'COLUMN_27, '); -- 증권발행여부, 청약형태, 만기구분
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 상품코드
                'COLUMN_29, ', -- 상품분류
                'COLUMN_30, '); -- 서명방법
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 상품코드
                'COLUMN_29, ', -- 상품분류
                'COLUMN_30, '); -- 상품코드, 상품분류, 서명방법

            -- 31-33
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_31, ', -- 보험종목코드
                'COLUMN_32, ', -- 실순갱신증번
                'COLUMN_33, '); -- 본인계약여부
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 보험종목코드
                'COLUMN_32, ', -- 실순갱신증번
                'COLUMN_33, '); -- 보험종목코드, 실순갱신증번, 본인계약여부
            
            -- 34-36
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_34, ', -- 수금원코드
                'COLUMN_35, ', -- 수금원명
                'NULL, '); -- 납기구분(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_34, ', -- 수금원코드
                'COLUMN_35, ', -- 수금원명
                'COLUMN_36, '); -- 수금원코드, 수금원명, 납기구분
            
            -- 37-38
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납입월(Target)
                'NULL'); -- 납입일(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_37, ', -- 납입월
                'COLUMN_38'); -- 납입월, 납입일

        -- Mapping for CAR (Columns 01-34)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 취급자코드
                'COLUMN_02, ', -- 취급자
                'COLUMN_03, '); -- 사용인코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 취급자코드
                'COLUMN_02, ', -- 취급자
                'COLUMN_03, '); -- 취급자코드, 취급자, 사용인코드
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 사용인
                'COLUMN_05, ', -- 팀기관
                'COLUMN_06, '); -- 피보험자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 사용인
                'COLUMN_05, ', -- 팀기관
                'COLUMN_06, '); -- 사용인, 팀기관, 피보험자
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 계약자
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 차량번호
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 계약자
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 계약자, 증권번호, 차량번호
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 보험상품
                'COLUMN_11, ', -- 플랜형
                'COLUMN_12, '); -- 마일리지특약
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 보험상품
                'COLUMN_11, ', -- 플랜형
                'COLUMN_12, '); -- 보험상품, 플랜형, 마일리지특약
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 보험시기
                'COLUMN_14, ', -- 보험종기
                'COLUMN_15, '); -- 표준율
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 보험시기
                'COLUMN_14, ', -- 보험종기
                'COLUMN_15, '); -- 보험시기, 보험종기, 표준율
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 발생구분
                'COLUMN_17, ', -- 납입회차
                'COLUMN_18, '); -- 보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 발생구분
                'COLUMN_17, ', -- 납입회차
                'COLUMN_18, '); -- 발생구분, 납입회차, 보험료
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 계상일자
                'COLUMN_20, ', -- 영수일자
                'COLUMN_21, '); -- 금종구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 계상일자
                'COLUMN_20, ', -- 영수일자
                'COLUMN_21, '); -- 계상일자, 영수일자, 금종구분
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 계약구분
                'COLUMN_23, ', -- 자율여부
                'COLUMN_24, '); -- 가입구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 계약구분
                'COLUMN_23, ', -- 자율여부
                'COLUMN_24, '); -- 계약구분, 자율여부, 가입구분
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 물건구분
                'COLUMN_26, ', -- 스캔여부
                'COLUMN_27, '); -- 비례소득
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 물건구분
                'COLUMN_26, ', -- 스캔여부
                'COLUMN_27, '); -- 물건구분, 스캔여부, 비례소득
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 청약형태
                'COLUMN_29, ', -- 전자서명
                'COLUMN_30, '); -- 영업속성
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 청약형태
                'COLUMN_29, ', -- 전자서명
                'COLUMN_30, '); -- 청약형태, 전자서명, 영업속성
            
            -- 31-33
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납기구분(Target)
                'NULL, ', -- 납입월(Target)
                'NULL, '); -- 납기(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 납기구분
                'COLUMN_32, ', -- 납입월
                'COLUMN_33, '); -- 납기구분, 납입월, 납기
            
            -- 34
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL'); -- 납입주기(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_34'); -- 납입주기

        -- Mapping for GEN (Columns 01-26)
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 모집자
                'COLUMN_02, ', -- 모집자명
                'COLUMN_03, '); -- 사용인
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 모집자
                'COLUMN_02, ', -- 모집자명
                'COLUMN_03, '); -- 모집자, 모집자명, 사용인
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 사용인명
                'COLUMN_05, ', -- 팀기관
                'COLUMN_06, '); -- 보험종목
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 사용인명
                'COLUMN_05, ', -- 팀기관
                'COLUMN_06, '); -- 사용인명, 팀기관, 보험종목
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 상품명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 상품코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 상품명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 상품명, 증권번호, 상품코드
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 계약자명
                'COLUMN_11, ', -- 보험시기
                'COLUMN_12, '); -- 보험종기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 계약자명
                'COLUMN_11, ', -- 보험시기
                'COLUMN_12, '); -- 계약자명, 보험시기, 보험종기
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 인수구분
                'COLUMN_14, ', -- 계상일자
                'COLUMN_15, '); -- 수납일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 인수구분
                'COLUMN_14, ', -- 계상일자
                'COLUMN_15, '); -- 인수구분, 계상일자, 수납일자
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 대리점분담
                'COLUMN_17, ', -- 비례소득
                'COLUMN_18, '); -- 영수보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 대리점분담
                'COLUMN_17, ', -- 비례소득
                'COLUMN_18, '); -- 대리점분담, 비례소득, 영수보험료
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 발생구분
                'COLUMN_20, ', -- 실환산P
                'NULL, '); -- 납기구분(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 발생구분
                'COLUMN_20, ', -- 실환산P
                'COLUMN_21, '); -- 발생구분, 실환산P, 납기구분
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납입월(Target)
                'NULL, ', -- 납기(Target)
                'NULL, '); -- 만기일자(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 납입월
                'COLUMN_23, ', -- 납기
                'COLUMN_24, '); -- 납입월, 납기, 만기일자
            
            -- 25-26
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'NULL, ', -- 납입방법(Target)
                'NULL'); -- 보험사성적(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 납입방법
                'COLUMN_26'); -- 납입방법, 보험사성적
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
                -- Rule 1: 맨 마지막열 값 추가(3개)
                -- ① 항목명I : 납기구분 / 항목값 : 년납
                -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                -- ③ 항목명III : 납입일 / 항목값 : 계상일자와 동일한 값으로 반영
                -- ※ 전체 행에 반영
                UPDATE T_TEMP_RPA_HWG_PROCESSED SET COLUMN_36 = '년납', COLUMN_37 = v_target_ym, COLUMN_38 = COLUMN_11;

                -- Rule 2: [월납환산보험료]="마이너스금액" 데이터 행삭제
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
                -- Rule 1: 맨 마지막열 값 추가(3개)
                -- ① 항목명I : 납기구분 / 항목값 : 년납
                -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
                -- ③ 항목명III : 납기 / 항목값 : 0
                -- ※ 전체 행에 반영

                UPDATE T_TEMP_RPA_HWG_PROCESSED SET COLUMN_21 = '년납', COLUMN_22 = v_target_ym, COLUMN_23 = '0';

                -- Rule 2:  [발생구분]="추징, 환급"이면 데이터 행삭제
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_19 IN ('추징', '환급');
                -- Rule 3: [발생구분]="해지" & [보험시기]≠"해당월"이면 데이터 행삭제s
                DELETE FROM T_TEMP_RPA_HWG_PROCESSED WHERE COLUMN_19 = '해지' AND LEFT(REPLACE(REPLACE(COLUMN_11, '-', ''), '.', ''), 6) <> v_target_ym;
                -- Rule 4: [보험료]="마이너스 금액"이면 데이터 행삭제
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
