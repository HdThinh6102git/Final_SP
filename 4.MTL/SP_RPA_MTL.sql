/*
 * SP_RPA_MTL
 * Description : Process MET Life insurance data
 * Parameters  :
 *   IN_BATCH_ID       : Batch ID to process
 *   IN_INSURANCE_TYPE : Insurance type (LIF)
 *   IN_CONTRACT_TYPE  : Contract type (NEW / EXT)
 * Steps       :
 *   1. Hardcoded column mapping by insurance type / contract type
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_MTL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'MTL';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT '';
    DECLARE v_proc_table VARCHAR(100) DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MTL_PROCESSED;
    END;

     -- Table Mapping by Insurance Type
    IF UPPER(IN_INSURANCE_TYPE) = 'LIF' THEN
        SET v_raw_table = 'T_RPA_LIFE_RAW';
        SET v_proc_table = 'T_RPA_LIFE_PROCESSED';
    END IF;

    -- 1. Hardcoded Column Mapping
    IF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 대리점
            'COLUMN_02, ', -- 지사명
            'COLUMN_03, '); -- 지사코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 대리점
            'COLUMN_02, ', -- 지사명
            'COLUMN_03, '); -- 지사코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 모집인성명
            'COLUMN_05, ', -- 모집인코드
            'COLUMN_06, '); -- 증번
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 모집인성명
            'COLUMN_05, ', -- 모집인코드
            'COLUMN_06, '); -- 증번

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 상태
            'COLUMN_08, ', -- 진단여부
            'COLUMN_09, '); -- 계약일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 상태
            'COLUMN_08, ', -- 진단여부
            'COLUMN_09, '); -- 계약일

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 계약자
            'COLUMN_11, ', -- 피보험자
            'COLUMN_12, '); -- 보종코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 계약자
            'COLUMN_11, ', -- 피보험자
            'COLUMN_12, '); -- 보종코드

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 상품명
            'COLUMN_14, ', -- 납입기간
            'COLUMN_15, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 상품명
            'COLUMN_14, ', -- 납입기간
            'COLUMN_15, '); -- 납입방법

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 초회납입
            'COLUMN_17, ', -- 수금방법
            'COLUMN_18, '); -- 자동이체일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 초회납입
            'COLUMN_17, ', -- 수금방법
            'COLUMN_18, '); -- 자동이체일

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 선납횟수
            'COLUMN_20, ', -- 1회P(KRW)
            'COLUMN_21, '); -- 1회P(USD)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 선납횟수
            'COLUMN_20, ', -- 1회P(KRW)
            'COLUMN_21, '); -- 1회P(USD)

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 가입금액(만원/USD)
            'COLUMN_23, ', -- 원화고정납 여부
            'COLUMN_24, '); -- 원화고정납입금액
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 가입금액(만원/USD)
            'COLUMN_23, ', -- 원화고정납 여부
            'COLUMN_24, '); -- 원화고정납입금액

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 성적인정1회P(원/KRW)
            'COLUMN_26, ', -- CSC(KRW)
            'COLUMN_27, '); -- CSC(USD)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 성적인정1회P(원/KRW)
            'COLUMN_26, ', -- CSC(KRW)
            'COLUMN_27, '); -- CSC(USD)

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 1차년도CSC
            'COLUMN_29, ', -- 2차년도CSC
            'COLUMN_30, '); -- 3차년도CSC
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 1차년도CSC
            'COLUMN_29, ', -- 2차년도CSC
            'COLUMN_30, '); -- 3차년도CSC

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 환율
            'COLUMN_32, ', -- 보장성여부
            'COLUMN_33, '); -- 법인계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 환율
            'COLUMN_32, ', -- 보장성여부
            'COLUMN_33, '); -- 법인계약여부

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 진단일자
            'COLUMN_35, ', -- 진단처
            'COLUMN_36, '); -- 진단결과날짜
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 진단일자
            'COLUMN_35, ', -- 진단처
            'COLUMN_36, '); -- 진단결과날짜

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 모집CSC(23.9월이후)
            'COLUMN_38, ', -- Gross ANP
            'NULL, '); -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 모집CSC(23.9월이후)
            'COLUMN_38, ', -- Gross ANP
            'COLUMN_39, '); -- 납기구분

        -- 40-41
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납입월
            'NULL');  -- 납입일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 납입월
            'COLUMN_41');  -- 납입일자

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) ='EXT' THEN
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 증번
            'COLUMN_02, ', -- 계약자
            'COLUMN_03, '); -- 피보험자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 증번
            'COLUMN_02, ', -- 계약자
            'COLUMN_03, '); -- 피보험자

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 보종코드
            'COLUMN_05, ', -- 상품명
            'COLUMN_06, '); -- 계약일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 보종코드
            'COLUMN_05, ', -- 상품명
            'COLUMN_06, '); -- 계약일자

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 만기일자
            'COLUMN_08, ', -- 납입주기
            'COLUMN_09, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 만기일자
            'COLUMN_08, ', -- 납입주기
            'COLUMN_09, '); -- 납입기간

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 수금방법
            'COLUMN_11, ', -- 자동이체희망일
            'COLUMN_12, '); -- 1차년CSC
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 수금방법
            'COLUMN_11, ', -- 자동이체희망일
            'COLUMN_12, '); -- 1차년CSC

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 2차년도CSC
            'COLUMN_14, ', -- 3차년도CSC
            'COLUMN_15, '); -- CSC
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 2차년도CSC
            'COLUMN_14, ', -- 3차년도CSC
            'COLUMN_15, '); -- CSC

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 보험료(KRW)
            'COLUMN_17, ', -- 보험료(USD)
            'COLUMN_18, '); -- 납초보험료(KRW)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 보험료(KRW)
            'COLUMN_17, ', -- 보험료(USD)
            'COLUMN_18, '); -- 납초보험료(KRW)

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 납초보험료(USD)
            'COLUMN_20, ', -- 가입금액(만원/USD)
            'COLUMN_21, '); -- 환율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 납초보험료(USD)
            'COLUMN_20, ', -- 가입금액(만원/USD)
            'COLUMN_21, '); -- 환율

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 소멸일자
            'COLUMN_23, ', -- 실납입횟수
            'COLUMN_24, '); -- 실납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 소멸일자
            'COLUMN_23, ', -- 실납입횟수
            'COLUMN_24, '); -- 실납입월

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 최종납입일자
            'COLUMN_26, ', -- 납입경로
            'COLUMN_27, '); -- 최종(유지)횟수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 최종납입일자
            'COLUMN_26, ', -- 납입경로
            'COLUMN_27, '); -- 최종(유지)횟수

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 최종유지년월
            'COLUMN_29, ', -- 연체여부
            'COLUMN_30, '); -- 계약상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 최종유지년월
            'COLUMN_29, ', -- 연체여부
            'COLUMN_30, '); -- 계약상태

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 계약상세상태
            'COLUMN_32, ', -- 모집영업소코드
            'COLUMN_33, '); -- 모집영업소명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 계약상세상태
            'COLUMN_32, ', -- 모집영업소코드
            'COLUMN_33, '); -- 모집영업소명

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 모집사원코드
            'COLUMN_35, ', -- 모집사원명
            'COLUMN_36, '); -- 수금영업소코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 모집사원코드
            'COLUMN_35, ', -- 모집사원명
            'COLUMN_36, '); -- 수금영업소코드

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 수금영업소명
            'COLUMN_38, ', -- 수금지사코드
            'COLUMN_39, '); -- 수금지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 수금영업소명
            'COLUMN_38, ', -- 수금지사코드
            'COLUMN_39, '); -- 수금지사명

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 수금사원코드
            'COLUMN_41, ', -- 수금사원명
            'COLUMN_42'); -- 등록주소지
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 수금사원코드
            'COLUMN_41, ', -- 수금사원명
            'COLUMN_42'); -- 등록주소지
    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MTL_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_MTL_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_MTL_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 2.3. Apply transformation rules
        IF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            -- Rule 1: 색필터 : 분홍색, 주황색 행삭제
            DELETE FROM T_TEMP_RPA_MTL_PROCESSED
            WHERE COLUMN_05 = '계';

            -- Rule 2: 값 편집
            -- ① [상태]=청약입력중 이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_MTL_PROCESSED
            WHERE COLUMN_07 = '청약입력중';

            -- ③ [납입기간]=“일시납” 이면 “0”으로 값수정
            UPDATE T_TEMP_RPA_MTL_PROCESSED
            SET COLUMN_14 = '0'
            WHERE COLUMN_14 = '일시납';

            -- Rule 3: 맨 마지막열 값 추가(3개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
            -- ③ 항목명III : 납입일자 / 항목값 : 계약일자와 동일한 값으로 반영
            -- ※ 전체 행에 반영
            UPDATE T_TEMP_RPA_MTL_PROCESSED
            SET COLUMN_39 = '년납',
                COLUMN_40 = DATE_FORMAT(CURDATE(), '%Y%m'),
                COLUMN_41 = COLUMN_09;

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'LIF' AND UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN

            -- Rule 1: [계약일자]=“2021.10월 이전” 계약은 증권번호 8자리로 변경(right 8)
            UPDATE T_TEMP_RPA_MTL_PROCESSED 
            SET COLUMN_01 = RIGHT(COLUMN_01, 8)
            WHERE REPLACE(REPLACE(LEFT(COLUMN_06, 7), '-', ''), '.', '') < '202110';

            -- Rule 2: [납입주기]=“일시납” 이면
            -- ① [보험료(KRW)]값을 “0”으로 수정
            -- ② [최종(유지)횟수]값을 “1”로 수정
            UPDATE T_TEMP_RPA_MTL_PROCESSED
            SET COLUMN_16 = '0',
                COLUMN_27 = '1'
            WHERE COLUMN_08 = '일시납';

            -- Rule 3: [만기일자]=“9999-02-29”이면 “9999-02-28”로 수정
            UPDATE T_TEMP_RPA_MTL_PROCESSED
            SET COLUMN_07 = '9999-02-28'
            WHERE COLUMN_07 = '9999-02-29';

            -- Rule 4: [증번]="마스킹"이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_MTL_PROCESSED
            WHERE COLUMN_01 = '마스킹';

            -- Rule 5: [계약상세상태]=“실효” & [최종납입일자]=“실효 3년 경과”면, [계약상태]값을 “시효”로 변경
            -- 3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하
            UPDATE T_TEMP_RPA_MTL_PROCESSED
            SET COLUMN_30 = '시효'
            WHERE COLUMN_31 = '실효'
              AND COLUMN_25 IS NOT NULL
              AND COLUMN_25 <> ''
              AND LEFT(REPLACE(REPLACE(COLUMN_25, '-', ''), '.', ''), 6) <= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 38 MONTH), '%Y%m');

        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_MTL_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MTL_PROCESSED;

    END IF;

END