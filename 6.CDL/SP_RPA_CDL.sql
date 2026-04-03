CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_CDL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols     TEXT         DEFAULT '';
    DECLARE v_proc_cols    TEXT         DEFAULT '';
    DECLARE v_sql_query    TEXT         DEFAULT '';
    DECLARE v_raw_table    VARCHAR(100) DEFAULT 'T_RPA_LIFE_RAW';
    DECLARE v_proc_table   VARCHAR(100) DEFAULT 'T_RPA_LIFE_PROCESSED';
    DECLARE v_row_count    INT          DEFAULT 0;
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'CDL';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_CDL_PROCESSED;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Rule 6 cutoff: 38 months
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Hardcoded Column Mapping for Chubb Life (CDL)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW contracts (Columns 01-38 + Target-only 39)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 계약자명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- NO, 증권번호, 계약자명
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 피보험자명
            'COLUMN_05, ', -- 상품코드
            'COLUMN_06, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 피보험자명
            'COLUMN_05, ', -- 상품코드
            'COLUMN_06, '); -- 상품명
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 계약상태
            'COLUMN_08, ', -- 계약일자
            'COLUMN_09, '); -- 계약변경일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 계약상태
            'COLUMN_08, ', -- 계약일자
            'COLUMN_09, '); -- 계약변경일자
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 가입금액
            'COLUMN_11, ', -- 보험료
            'COLUMN_12, '); -- 합계환산성적
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 가입금액
            'COLUMN_11, ', -- 보험료
            'COLUMN_12, '); -- 합계환산성적
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 1차년 환산성적
            'COLUMN_14, ', -- 2차년 환산성적
            'COLUMN_15, '); -- 3차년 환산성적
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 1차년 환산성적
            'COLUMN_14, ', -- 2차년 환산성적
            'COLUMN_15, '); -- 3차년 환산성적
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 4차년 환산성적
            'COLUMN_17, ', -- 합계환산율
            'COLUMN_18, '); -- 1차년환산율
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 4차년 환산성적
            'COLUMN_17, ', -- 합계환산율
            'COLUMN_18, '); -- 1차년환산율
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 2차년환산율
            'COLUMN_20, ', -- 3차년환산율
            'COLUMN_21, '); -- 4차년환산율
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 2차년환산율
            'COLUMN_20, ', -- 3차년환산율
            'COLUMN_21, '); -- 4차년환산율
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 보험기간
            'COLUMN_23, ', -- 납입기간
            'COLUMN_24, '); -- 납입주기
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 보험기간
            'COLUMN_23, ', -- 납입기간
            'COLUMN_24, '); -- 납입주기
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 납입방법
            'COLUMN_26, ', -- 출금요청일
            'COLUMN_27, '); -- 납입일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 납입방법
            'COLUMN_26, ', -- 출금요청일
            'COLUMN_27, '); -- 납입일자
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 납입월
            'COLUMN_29, ', -- 최종납입월
            'COLUMN_30, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 납입월
            'COLUMN_29, ', -- 최종납입월
            'COLUMN_30, '); -- 납입회차
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 모집인코드
            'COLUMN_32, ', -- 모집인명
            'COLUMN_33, '); -- 수금인코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 모집인코드
            'COLUMN_32, ', -- 모집인명
            'COLUMN_33, '); -- 수금인코드
        
        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_34, ', -- 수금인명
            'COLUMN_35, ', -- 대리점
            'COLUMN_36, '); -- 지점
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_34, ', -- 수금인명
            'COLUMN_35, ', -- 대리점
            'COLUMN_36, '); -- 지점
        
        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_37, ', -- 지사
            'COLUMN_38, ', -- 피보험자 연령(가입당시)
            'NULL'); -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_37, ', -- 지사
            'COLUMN_38, ', -- 피보험자 연령(가입당시)
            'COLUMN_39'); -- 납기구분

    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        -- Mapping for EXT contracts (Columns 01-40)
        SET v_raw_cols = ''; SET v_proc_cols = '';
        
        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 계약자명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_01, ', -- NO
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 계약자명
        
        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_04, ', -- 피보험자명
            'COLUMN_05, ', -- 상품코드
            'COLUMN_06, '); -- 상품명
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_04, ', -- 피보험자명
            'COLUMN_05, ', -- 상품코드
            'COLUMN_06, '); -- 상품명
        
        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_07, ', -- 계약상태
            'COLUMN_08, ', -- 계약일자
            'COLUMN_09, '); -- 계약변경일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_07, ', -- 계약상태
            'COLUMN_08, ', -- 계약일자
            'COLUMN_09, '); -- 계약변경일자
        
        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_10, ', -- 가입금액
            'COLUMN_11, ', -- 보험료
            'COLUMN_12, '); -- 합계환산성적
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_10, ', -- 가입금액
            'COLUMN_11, ', -- 보험료
            'COLUMN_12, '); -- 합계환산성적
        
        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_13, ', -- 1차년 환산성적
            'COLUMN_14, ', -- 2차년 환산성적
            'COLUMN_15, '); -- 3차년 환산성적
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_13, ', -- 1차년 환산성적
            'COLUMN_14, ', -- 2차년 환산성적
            'COLUMN_15, '); -- 3차년 환산성적
        
        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_16, ', -- 4차년 환산성적
            'COLUMN_17, ', -- 합계환산율
            'COLUMN_18, '); -- 1차년환산율
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_16, ', -- 4차년 환산성적
            'COLUMN_17, ', -- 합계환산율
            'COLUMN_18, '); -- 1차년환산율
        
        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_19, ', -- 2차년환산율
            'COLUMN_20, ', -- 3차년환산율
            'COLUMN_21, '); -- 4차년환산율
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_19, ', -- 2차년환산율
            'COLUMN_20, ', -- 3차년환산율
            'COLUMN_21, '); -- 4차년환산율
        
        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_22, ', -- 보험기간
            'COLUMN_23, ', -- 납입기간
            'COLUMN_24, '); -- 납입주기
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_22, ', -- 보험기간
            'COLUMN_23, ', -- 납입기간
            'COLUMN_24, '); -- 납입주기
        
        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_25, ', -- 납입방법
            'COLUMN_26, ', -- 출금요청일
            'COLUMN_27, '); -- 납입일자
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_25, ', -- 납입방법
            'COLUMN_26, ', -- 출금요청일
            'COLUMN_27, '); -- 납입일자
        
        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_28, ', -- 납입월
            'COLUMN_29, ', -- 최종납입월
            'COLUMN_30, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_28, ', -- 납입월
            'COLUMN_29, ', -- 최종납입월
            'COLUMN_30, '); -- 납입회차
        
        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_31, ', -- 모집인코드
            'COLUMN_32, ', -- 모집인명
            'COLUMN_33, '); -- 수금인코드
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_31, ', -- 모집인코드
            'COLUMN_32, ', -- 모집인명
            'COLUMN_33, '); -- 수금인코드
        
        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_34, ', -- 수금인명
            'COLUMN_35, ', -- 수금인위해촉여부
            'COLUMN_36, '); -- 수금인연락처
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_34, ', -- 수금인명
            'COLUMN_35, ', -- 수금인위해촉여부
            'COLUMN_36, '); -- 수금인연락처
        
        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols, 
            'COLUMN_37, ', -- 수금대리점
            'COLUMN_38, ', -- 수금지점
            'COLUMN_39, '); -- 수금지사
        SET v_proc_cols = CONCAT(v_proc_cols, 
            'COLUMN_37, ', -- 수금대리점
            'COLUMN_38, ', -- 수금지점
            'COLUMN_39, '); -- 수금지사
        
        -- 40
        SET v_raw_cols = CONCAT(v_raw_cols, 'COLUMN_40'); -- 피보험자 연령(가입당시)
        SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_40'); -- 피보험자 연령(가입당시)
    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_CDL_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_CDL_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_CDL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''CDL'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''CDL'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_01 <> ''NO'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 맨 마지막열 값 추가(1개)
            -- ① 항목명 : 납기구분 / 항목값 : 년납
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_39 = '년납';

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
            -- Rule 1: [계약상태]=“정상(유지),실효,정상화기간,효력상실”이면 [계약변경일]을 “0000-00-00”으로 수정
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_09 = '0000-00-00'
            WHERE COLUMN_07 IN ('정상(유지)', '실효', '정상화기간', '효력상실');

            -- Rule 2: [납입일자]=“빈값”이면, [계약일자]로 수정
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_27 = COLUMN_08
            WHERE COLUMN_27 IS NULL OR COLUMN_27 = '';

            -- Rule 3: [최종납입월]=“빈값＂이면, [계약일자]로 수정
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_29 = COLUMN_08
            WHERE COLUMN_29 IS NULL OR COLUMN_29 = '';

            -- Rule 4: [납입회차]=“0”이면, “1”로 수정
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_30 = '1' WHERE COLUMN_30 = '0';

            -- Rule 5: [납입주기]=“일시납” 이면
            -- ① [보험료]값을 “0”으로 수정
            -- ② [납입회차]값을 “1”로 수정
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_11 = '0', COLUMN_30 = '1'
            WHERE COLUMN_24 = '일시납';

            -- Rule 6: [계약상태]=“실효” & [최종납입월]=“실효 3년 경과”면, [계약상태]값을 “시효”로 변경
            -- 3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하
            UPDATE T_TEMP_RPA_CDL_PROCESSED SET COLUMN_07 = '시효'
            WHERE COLUMN_07 = '실효' 
              AND COLUMN_29 IS NOT NULL AND COLUMN_29 <> ''
              AND LEFT(REPLACE(REPLACE(COLUMN_29, '-', ''), '.', ''), 6) <= v_cutoff_ym;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_CDL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_CDL_PROCESSED;

    END IF;

END