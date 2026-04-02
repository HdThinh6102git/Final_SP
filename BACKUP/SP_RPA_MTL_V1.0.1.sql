CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_MTL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- DECLARE variables
    DECLARE v_row_count  INT        DEFAULT 0;
    DECLARE v_company_code VARCHAR(10) DEFAULT 'MTL';
    DECLARE v_target_ym  VARCHAR(6) DEFAULT '';
    DECLARE v_cutoff_ym  VARCHAR(6) DEFAULT '';

    -- DECLARE handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
    END;

    -- SET / logic
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Rule 5 cutoff: 38 months
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Create temporary table
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
    CREATE TEMPORARY TABLE T_TEMP_RPA_LIFE_PROCESSED LIKE T_RPA_LIFE_PROCESSED;

    -- ======================================================================
    --  CONTRACT TYPE : NEW (신계약) - 38 Columns Mapping
    -- ======================================================================
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        
        INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            -- 대리점 | 지사명 | 지사코드 | 모집인성명 | 모집인코드
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, 
            -- 증번 | 상태 | 진단여부 | 계약일 | 계약자
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, 
            -- 피보험자 | 보종코드 | 상품명 | 납입기간 | 납입방법
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, 
            -- 초회납입 | 수금방법 | 자동이체일 | 선납횟수 | 1회P(KRW)
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, 
            -- 1회P(USD) | 가입금액 | 원화고정여부 | 원화고정금액 | 성적인정1회P
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, 
            -- CSC(KRW) | CSC(USD) | 1차년도CSC | 2차년도CSC | 3차년도CSC
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30, 
            -- 환율 | 보장성여부 | 법인계약여부 | 진단일자 | 진단처
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, 
            -- 진단결과날짜 | 모집CSC | Gross ANP
            COLUMN_36, COLUMN_37, COLUMN_38
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

        -- [Rule 1 & 2-1] Delete Summary rows (모집인코드='계') & Specific status (상태='청약심사')
        DELETE FROM T_TEMP_RPA_LIFE_PROCESSED 
        WHERE (COLUMN_05 = '계') OR (COLUMN_07 = '청약심사');

        -- [Rule 2-2] 납입방법 (15) = '일시납' -> Update 납입기간 (14) = 0
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_14 = '0' WHERE COLUMN_15 = '일시납';

        -- [Rule 3] Add Extra Columns: 납기구분(39), 납입월(40), 납입일자(41)
        UPDATE T_TEMP_RPA_LIFE_PROCESSED 
        SET COLUMN_39 = '년납', 
            COLUMN_40 = v_target_ym, 
            COLUMN_41 = COLUMN_09
        WHERE UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

    -- ======================================================================
    --  CONTRACT TYPE : EXT (Existing Contract) - 42 Columns Mapping
    -- ======================================================================
    ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') THEN
        
        INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40,
            COLUMN_41, COLUMN_42
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            -- 증번 | 계약자 | 피보험자 | 보종코드 | 상품명
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, 
            -- 계약일자 | 만기일자 | 납입주기 | 납입기간 | 수금방법
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, 
            -- 자동이체희망일 | 1차년CSC | 2차년도CSC | 3차년도CSC | CSC
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, 
            -- 보험료(KRW) | 보험료(USD) | 납초보험료(KRW) | 납초보험료(USD) | 가입금액
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, 
            -- 환율 | 소멸일자 | 실납입횟수 | 실납입월 | 최종납입일자
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, 
            -- 납입경로 | 최종(유지)횟수 | 최종유지년월 | 연체여부 | 계약상태
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30, 
            -- 계약상세상태 | 모집영업소코드 | 모집영업소명 | 모집사원코드 | 모집사원명
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, 
            -- 수금영업소코드 | 수금영업소명 | 수금지사코드 | 수금지사명 | 수금사원코드 | 수금사원명 | 등록주소지
            COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40, COLUMN_41, COLUMN_42
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

        -- [Rule 4] Delete Masking rows
        DELETE FROM T_TEMP_RPA_LIFE_PROCESSED WHERE COLUMN_01 = '마스킹';

        -- [Rule 1] Contract Date before Oct 2021 -> 증권번호 = 8 digits (Right)
        UPDATE T_TEMP_RPA_LIFE_PROCESSED 
        SET COLUMN_01 = RIGHT(COLUMN_01, 8)
        WHERE REPLACE(REPLACE(LEFT(COLUMN_06, 7), '-', ''), '.', '') < '202110';

        -- [Rule 2] 납입주기 (8) = '일시납' -> 보험료(KRW) (16)=0, 최종(유지)횟수 (27)=1
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_16 = '0', COLUMN_27 = '1' WHERE COLUMN_08 = '일시납';

        -- [Rule 3] 만기일자 (7) = '9999-02-29' -> '9999-02-28'
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_07 = '9999-02-28' WHERE COLUMN_07 = '9999-02-29';

        -- [Rule 5] 상세상태 (31) = '실효' AND 최종납입일자 (25) <= 38 months -> 상태 (30) = '시효'
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_30 = '시효'
        WHERE COLUMN_31 = '실효'
          AND COLUMN_25 IS NOT NULL AND COLUMN_25 <> ''
          AND LEFT(REPLACE(REPLACE(COLUMN_25, '-', ''), '.', ''), 6) <= v_cutoff_ym;

    END IF;

    -- ======================================================================
    --  FINAL INSERT TO PROCESSED
    -- ======================================================================
    INSERT INTO T_RPA_LIFE_PROCESSED (
        SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
        COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40,
        COLUMN_41, COLUMN_42
    )
    SELECT
        SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
        COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40,
        COLUMN_41, COLUMN_42
    FROM T_TEMP_RPA_LIFE_PROCESSED;

    SET v_row_count = ROW_COUNT();
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;

END