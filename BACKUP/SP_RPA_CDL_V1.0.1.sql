CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_CDL`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- DECLARE variables
    DECLARE v_row_count  INT        DEFAULT 0;
    DECLARE v_company_code VARCHAR(10) DEFAULT 'CDL';
    DECLARE v_target_ym  VARCHAR(6) DEFAULT '';
    DECLARE v_cutoff_ym  VARCHAR(6) DEFAULT '';

    -- DECLARE handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;
    END;

    -- SET / logic
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- Rule 6 cutoff: 38 months
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
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, -- NO, 증권번호, 계약자명, 피보험자, 상품코드
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, -- 상품명, 계약상태, 계약일자, 변경일자, 가입금액
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, -- 보험료, 합성적, 1성적, 2성적, 3성적
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, -- 4성적, 합환산율, 1환산율, 2환산율, 3환산율
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, -- 4환산율, 보험기간, 납입기간, 납입주기, 납입방법
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30, -- 출금요청, 납입일자, 납입월, 최종납월, 납입회차
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, -- 모집코드, 모집인명, 수금코드, 수금인명, 대리점
            COLUMN_36, COLUMN_37, COLUMN_38                         -- 지점, 지사, 피보험연령
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = 'NEW';

        -- [Rule 1] 맨 마지막열 값 추가 (납기구분 = 년납)
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_39 = '년납' WHERE UPPER(CONTRACT_TYPE) = 'NEW';

    -- ======================================================================
    --  CONTRACT TYPE : EXT (Existing Contract) - 40 Columns Mapping
    -- ======================================================================
    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        
        INSERT INTO T_TEMP_RPA_LIFE_PROCESSED (
            SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40
        )
        SELECT
            REPLACE(UUID(), '-', ''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), v_company_code, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
            COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, -- NO, 증권번호, 계약자명, 피보험자, 상품코드
            COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10, -- 상품명, 계약상태, 계약일자, 변경일자, 가입금액
            COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, -- 보험료, 합성적, 1성적, 2성적, 3성적
            COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20, -- 4성적, 합환산율, 1환산율, 2환산율, 3환산율
            COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, -- 4환산율, 보험기간, 납입기간, 납입주기, 납입방법
            COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30, -- 출금요청, 납입일자, 납입월, 최종납월, 납입회차
            COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, -- 모집코드, 모집인명, 수금코드, 수금인명, 위해촉여부
            COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40  -- 연락처, 수금대리, 수금지점, 수금지사, 피보험연령
        FROM T_RPA_LIFE_RAW
        WHERE COMPANY_CODE = v_company_code AND BATCH_ID = IN_BATCH_ID AND UPPER(CONTRACT_TYPE) = UPPER(IN_CONTRACT_TYPE);

        -- [Rule 1] [계약상태]="정상(유지),실효,정상화기간,효력상실"이면 [계약변경일자](09)를 "0000-00-00"으로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_09 = '0000-00-00'
        WHERE COLUMN_07 IN ('정상(유지)', '실효', '정상화기간', '효력상실');

        -- [Rule 2] [납입일자](27)="빈값"이면, [계약일자](08)로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_27 = COLUMN_08
        WHERE COLUMN_27 IS NULL OR COLUMN_27 = '';

        -- [Rule 3] [최종납입월](29)="빈값"이면, [계약일자](08)로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_29 = COLUMN_08
        WHERE COLUMN_29 IS NULL OR COLUMN_29 = '';

        -- [Rule 4] [납입회차](30)="0"이면, "1"로 수정
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_30 = '1' WHERE COLUMN_30 = '0';

        -- [Rule 5] [납입주기](24)="일시납" -> (1) [보험료](11)="0" (2) [납입회차](30)="1"
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_11 = '0', COLUMN_30 = '1'
        WHERE COLUMN_24 = '일시납';

        -- [Rule 6] [계약상태](07)="실효" & [최종납입월](29) 38개월 경과 -> "시효"
        UPDATE T_TEMP_RPA_LIFE_PROCESSED SET COLUMN_07 = '시효'
        WHERE COLUMN_07 = '실효' 
          AND COLUMN_29 IS NOT NULL AND COLUMN_29 <> ''
          AND LEFT(REPLACE(REPLACE(COLUMN_29, '-', ''), '.', ''), 6) <= v_cutoff_ym;

    END IF;

    -- ======================================================================
    --  FINAL INSERT TO PROCESSED (Compact format)
    -- ======================================================================
    INSERT INTO T_RPA_LIFE_PROCESSED (
        SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
        COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40
    )
    SELECT
        SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX,
        COLUMN_01, COLUMN_02, COLUMN_03, COLUMN_04, COLUMN_05, COLUMN_06, COLUMN_07, COLUMN_08, COLUMN_09, COLUMN_10,
        COLUMN_11, COLUMN_12, COLUMN_13, COLUMN_14, COLUMN_15, COLUMN_16, COLUMN_17, COLUMN_18, COLUMN_19, COLUMN_20,
        COLUMN_21, COLUMN_22, COLUMN_23, COLUMN_24, COLUMN_25, COLUMN_26, COLUMN_27, COLUMN_28, COLUMN_29, COLUMN_30,
        COLUMN_31, COLUMN_32, COLUMN_33, COLUMN_34, COLUMN_35, COLUMN_36, COLUMN_37, COLUMN_38, COLUMN_39, COLUMN_40
    FROM T_TEMP_RPA_LIFE_PROCESSED;

    SET v_row_count = ROW_COUNT();
    DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LIFE_PROCESSED;

END