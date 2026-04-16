/*
 * SP_RPA_MRF
 * Description : Process Meritz Fire insurance data
 * Parameters  :
 *   IN IN_BATCH_ID       : Batch ID to process
 *   IN IN_INSURANCE_TYPE : Insurance type (LTR / CAR / GEN)
 *   IN IN_CONTRACT_TYPE  : Contract type (NEW / EXT)
 * Steps       :
 *   1. Hardcoded column mapping by insurance type / contract type
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_MRF`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'MRF';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT '';
    DECLARE v_processed_table VARCHAR(100) DEFAULT '';

    -- [DECLARE debug variables]
    DECLARE v_log_initial_raw  INT DEFAULT 0;
    DECLARE v_log_temp_initial INT DEFAULT 0;
    DECLARE v_log_after_rule1  INT DEFAULT 0;
    DECLARE v_log_after_rule2  INT DEFAULT 0;
    DECLARE v_log_after_rule3  INT DEFAULT 0;
    DECLARE v_log_after_rule4  INT DEFAULT 0;
    DECLARE v_log_after_rule5  INT DEFAULT 0;
    DECLARE v_log_after_rule6  INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @v_err_no = MYSQL_ERRNO,
            @v_err_msg = MESSAGE_TEXT;

        INSERT INTO T_RPA_DEBUG_LOG VALUES (
            IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
            CONCAT('SQL_EXCEPTION: [', @v_err_no, ']'),
            0, NOW()
        );

        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_mrf_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_mrf_del_case;
    END;

    -- [INIT Debug Log Table]
    CREATE TABLE IF NOT EXISTS T_RPA_DEBUG_LOG (
        BATCH_ID       VARCHAR(100),
        COMPANY_CODE   VARCHAR(10),
        INSURANCE_TYPE VARCHAR(50),
        CONTRACT_TYPE  VARCHAR(20),
        STEP_NAME      VARCHAR(100),
        ROW_COUNT      INT,
        LOG_TIME       DATETIME
    );

    -- 1. Hardcoded Column Mapping
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' AND UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';
        SET v_raw_cols = '';
        SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 기업RM코드
            'COLUMN_24, '); -- 기업RM명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 기업RM코드
            'COLUMN_24, '); -- 기업RM명

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 기업총무코드
            'COLUMN_26, ', -- 기업총무명
            'COLUMN_27, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 기업총무코드
            'COLUMN_26, ', -- 기업총무명
            'COLUMN_27, '); -- 상품코드

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 상품명
            'COLUMN_29, ', -- 고위험물건
            'COLUMN_30, '); -- 공동물건
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 상품명
            'COLUMN_29, ', -- 고위험물건
            'COLUMN_30, '); -- 공동물건

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 행복나눔특약
            'COLUMN_32, ', -- 청약일
            'COLUMN_33, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 행복나눔특약
            'COLUMN_32, ', -- 청약일
            'COLUMN_33, '); -- 납입기간

        -- 34-35
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 보험기간
            'COLUMN_35, ', -- 보험종료일자
            'COLUMN_36, ');  -- 인수형태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 보험기간
            'COLUMN_35, ', -- 보험종료일자
            'COLUMN_36, ');  -- 인수형태

        -- 37-38
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 피보험자
            'NULL, ', -- 납기구분
            'NULL');  -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 피보험자
            'COLUMN_38, ', -- 납기구분
            'COLUMN_39');  -- 납입월

    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'NEW' AND UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        SET v_raw_table = 'T_RPA_CAR_RAW';
        SET v_processed_table = 'T_RPA_CAR_PROCESSED';
        SET v_raw_cols = '';
        SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 기업RM코드
            'COLUMN_24, '); -- 기업RM명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 기업RM코드
            'COLUMN_24, '); -- 기업RM명

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 기업총무코드
            'COLUMN_26, ', -- 기업총무명
            'COLUMN_27, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 기업총무코드
            'COLUMN_26, ', -- 기업총무명
            'COLUMN_27, '); -- 상품코드

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 상품명
            'COLUMN_29, ', -- 고위험물건
            'COLUMN_30, '); -- 공동물건
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 상품명
            'COLUMN_29, ', -- 고위험물건
            'COLUMN_30, '); -- 공동물건

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 행복나눔특약
            'COLUMN_32, ', -- 청약일
            'COLUMN_33, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 행복나눔특약
            'COLUMN_32, ', -- 청약일
            'COLUMN_33, '); -- 납입기간

        -- 34-35
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 보험기간
            'COLUMN_35, ', -- 보험종료일자
            'COLUMN_36, ');  -- 인수형태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 보험기간
            'COLUMN_35, ', -- 보험종료일자
            'COLUMN_36, ');  -- 인수형태

        -- 37-38
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 피보험자
            'NULL, ', -- 납기구분
            'NULL, ', -- 납입월
            'NULL');  -- 보험기간 시작일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 피보험자
            'COLUMN_38, ', -- 납기구분
            'COLUMN_39, ', -- 납입월
            'COLUMN_40');  -- 보험기간 시작일

    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'NEW' AND UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        SET v_raw_table = 'T_RPA_GENERAL_RAW';
        SET v_processed_table = 'T_RPA_GENERAL_PROCESSED';
        SET v_raw_cols = '';
        SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 일자
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 보험료

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 납입주기
            'COLUMN_05, ', -- 횟수
            'COLUMN_06, '); -- 수금방법

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 영수
            'COLUMN_08, ', -- 선납
            'COLUMN_09, '); -- 초회수정P

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 초년도수정P
            'COLUMN_11, ', -- 출생후보험료
            'COLUMN_12, '); -- 출생후수정P

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 수정P
            'COLUMN_14, ', -- 계약자
            'COLUMN_15, '); -- 수수료분급

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 신규구분
            'COLUMN_17, ', -- 차량번호
            'COLUMN_18, '); -- 입력일

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 리스크등급
            'COLUMN_20, ', -- 대리점설계사코드
            'COLUMN_21, '); -- 대리점설계사명

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 기업RM코드
            'COLUMN_24, '); -- 기업RM명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 지사명
            'COLUMN_23, ', -- 기업RM코드
            'COLUMN_24, '); -- 기업RM명

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 기업총무코드
            'COLUMN_26, ', -- 기업총무명
            'COLUMN_27, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 기업총무코드
            'COLUMN_26, ', -- 기업총무명
            'COLUMN_27, '); -- 상품코드

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 상품명
            'COLUMN_29, ', -- 고위험물건
            'COLUMN_30, '); -- 공동물건
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 상품명
            'COLUMN_29, ', -- 고위험물건
            'COLUMN_30, '); -- 공동물건

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 행복나눔특약
            'COLUMN_32, ', -- 청약일
            'COLUMN_33, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 행복나눔특약
            'COLUMN_32, ', -- 청약일
            'COLUMN_33, '); -- 납입기간

        -- 34-35
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 보험기간
            'COLUMN_35, ', -- 보험종료일자
            'COLUMN_36, ');  -- 인수형태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 보험기간
            'COLUMN_35, ', -- 보험종료일자
            'COLUMN_36, ');  -- 인수형태

        -- 37-38
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 피보험자
            'NULL, ', -- 납기구분
            'NULL, ', -- 납입월
            'NULL');  -- 보험기간 시작일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 피보험자
            'COLUMN_38, ', -- 납기구분
            'COLUMN_39, ', -- 납입월
            'COLUMN_40');  -- 보험기간 시작일

    ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' AND UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';
        SET v_raw_cols = '';
        SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 업무시스템코드
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 단위상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 업무시스템코드
            'COLUMN_02, ', -- 증권번호
            'COLUMN_03, '); -- 단위상품코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 청약일자
            'COLUMN_05, ', -- 계약상태코드
            'COLUMN_06, '); -- 보험개시일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 청약일자
            'COLUMN_05, ', -- 계약상태코드
            'COLUMN_06, '); -- 보험개시일자

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 보험종료일자
            'COLUMN_08, ', -- 최종납입년월
            'COLUMN_09, '); -- 최종납입회차
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 보험종료일자
            'COLUMN_08, ', -- 최종납입년월
            'COLUMN_09, '); -- 최종납입회차

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 영업보험료금액
            'COLUMN_11, ', -- 총납입보험료금액
            'COLUMN_12, '); -- 취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 영업보험료금액
            'COLUMN_11, ', -- 총납입보험료금액
            'COLUMN_12, '); -- 취급자코드

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 취급자명
            'COLUMN_14, ', -- 대리점설계사코드
            'COLUMN_15, '); -- 대리점설계사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 취급자명
            'COLUMN_14, ', -- 대리점설계사코드
            'COLUMN_15, '); -- 대리점설계사명

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 대리점지사코드
            'COLUMN_17, ', -- 대리점지사명
            'COLUMN_18, '); -- 취급본부조직명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 대리점지사코드
            'COLUMN_17, ', -- 대리점지사명
            'COLUMN_18, '); -- 취급본부조직명

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 취급본부조직코드
            'COLUMN_20, ', -- 취급지역단조직명
            'COLUMN_21, '); -- 취급지역단조직코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 취급본부조직코드
            'COLUMN_20, ', -- 취급지역단조직명
            'COLUMN_21, '); -- 취급지역단조직코드

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 취급지점조직명
            'COLUMN_23, ', -- 취급지점조직코드
            'COLUMN_24, '); -- 관리본부조직명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 취급지점조직명
            'COLUMN_23, ', -- 취급지점조직코드
            'COLUMN_24, '); -- 관리본부조직명

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 관리본부조직코드
            'COLUMN_26, ', -- 관리지역단조직명
            'COLUMN_27, '); -- 관리지역단조직코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 관리본부조직코드
            'COLUMN_26, ', -- 관리지역단조직명
            'COLUMN_27, '); -- 관리지역단조직코드

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 관리지점조직명
            'COLUMN_29, ', -- 관리지점조직코드
            'COLUMN_30, '); -- 모집인조직코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 관리지점조직명
            'COLUMN_29, ', -- 관리지점조직코드
            'COLUMN_30, '); -- 모집인조직코드

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 모집인조직명
            'COLUMN_32, ', -- 모집대리점설계사조직코드
            'COLUMN_33, '); -- 모집대리점설계사조직명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 모집인조직명
            'COLUMN_32, ', -- 모집대리점설계사조직코드
            'COLUMN_33, '); -- 모집대리점설계사조직명

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 모집본부명
            'COLUMN_35, ', -- 모집본부코드
            'COLUMN_36, '); -- 모집지역단명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 모집본부명
            'COLUMN_35, ', -- 모집본부코드
            'COLUMN_36, '); -- 모집지역단명

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 모집지역단코드
            'COLUMN_38, ', -- 모집지점명
            'COLUMN_39, '); -- 모집지점코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 모집지역단코드
            'COLUMN_38, ', -- 모집지점명
            'COLUMN_39, '); -- 모집지점코드

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 단위상품명
            'COLUMN_41, ', -- 보험기간유형코드
            'COLUMN_42, '); -- 보험기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 단위상품명
            'COLUMN_41, ', -- 보험기간유형코드
            'COLUMN_42, '); -- 보험기간

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 보험종료연령
            'COLUMN_44, ', -- 납입기간유형코드
            'COLUMN_45, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 보험종료연령
            'COLUMN_44, ', -- 납입기간유형코드
            'COLUMN_45, '); -- 납입기간

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 납입종료연령
            'COLUMN_47, ', -- 납입주기코드
            'COLUMN_48, '); -- 수금방법코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 납입종료연령
            'COLUMN_47, ', -- 납입주기코드
            'COLUMN_48, '); -- 수금방법코드

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 계약자명
            'COLUMN_50, ', -- 피보험자순번
            'COLUMN_51, '); -- 피보험자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 계약자명
            'COLUMN_50, ', -- 피보험자순번
            'COLUMN_51, '); -- 피보험자명

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 피보험자생년월일
            'COLUMN_53, ', -- 계약상태명
            'COLUMN_54, '); -- 소멸실효일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 피보험자생년월일
            'COLUMN_53, ', -- 계약상태명
            'COLUMN_54, '); -- 소멸실효일자

        -- 55-56
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 최종납입일자
            'COLUMN_56');  -- 계약상태상세명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 최종납입일자
            'COLUMN_56');  -- 계약상태상세명
    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' AND v_processed_table != '' THEN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_MRF_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_MRF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET @sql_raw_count = CONCAT(
            'SELECT COUNT(*) INTO @v_raw_count FROM ', v_raw_table,
            ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' ',
            'AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            'AND COMPANY_CODE = ''', v_company_code, ''''
        );
        PREPARE stmt_raw FROM @sql_raw_count;
        EXECUTE stmt_raw;
        DEALLOCATE PREPARE stmt_raw;
        SET v_log_initial_raw = @v_raw_count;

        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_MRF_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' AND UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            -- Rule 1: 맨 마지막열 값 추가(2개)
            -- ① 항목명I : 납기구분 / 항목값 : 년납
            -- ② 항목명II : 납입월 / 항목값 : 해당월
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_38 = '년납',
                COLUMN_39 = DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: [증권번호] 오름차순 정렬 후 [영수]≠"신계약, 취소"면 데이터 행삭제
            SET @seq := 0;
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_02 ASC, EXCEL_ROW_INDEX ASC;

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_07 NOT IN ('신계약', '취소');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            -- Rule 3: [일자]≠해당월 & [상품명]≠실손이면 데이터 행삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE LEFT(REPLACE(COLUMN_01, '-', ''), 6) <> DATE_FORMAT(CURDATE(), '%Y%m')
              AND COLUMN_28 NOT LIKE '%실손%';

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_3', v_log_after_rule3, NOW());

            -- Rule 4: 증권번호 중복 편집
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_dup_case;
            CREATE TEMPORARY TABLE tmp_mrf_dup_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING SUM(CASE WHEN COLUMN_07 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_07 = '취소' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_MRF_PROCESSED t
            INNER JOIN tmp_mrf_dup_case d ON t.COLUMN_02 = d.COLUMN_02
            SET t.COLUMN_07 = '취소';
            
            -- 중복 증권번호 중 Delete the row where [보험료] is a negative value.
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_dup_case)
              AND REPLACE(IFNULL(COLUMN_03, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_4', v_log_after_rule4, NOW());

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'NEW' AND UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            -- Rule 1: 맨 마지막열 값 추가(2개)
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_38 = '년납',
                COLUMN_39 = DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2.1: 계약번호 오름차순 정렬
            SET @seq := 0;
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_02 ASC, EXCEL_ROW_INDEX ASC;

            -- Rule 2.2: 중복 계약번호 중 [영수]=모두 "배서"면 해당 데이터들 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_del_case;
            CREATE TEMPORARY TABLE tmp_mrf_del_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_07 <> '배서' THEN 1 ELSE 0 END) = 0;

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_del_case);

            -- Rule 2.3: 중복 계약번호 중 [영수]=각각"신계약,배서"면 "배서" 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_del_case;
            CREATE TEMPORARY TABLE tmp_mrf_del_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING SUM(CASE WHEN COLUMN_07 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_07 = '배서' THEN 1 ELSE 0 END) > 0;

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_del_case)
              AND COLUMN_07 = '배서';

            -- Rule 2.4: 중복 계약번호 중 [영수]=각각"신계약,취소"면 [영수]="취소"로 수정 및 [보험료]="마이너스금액" 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_dup_case;
            CREATE TEMPORARY TABLE tmp_mrf_dup_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING SUM(CASE WHEN COLUMN_07 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_07 = '취소' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_MRF_PROCESSED t
            INNER JOIN tmp_mrf_dup_case d ON t.COLUMN_02 = d.COLUMN_02
            SET t.COLUMN_07 = '취소';

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_dup_case)
              AND REPLACE(IFNULL(COLUMN_03, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            -- Rule 3: [보험료]="마이너스"이면 "플러스"값으로 수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_03 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_03, '0'), ',', '') AS SIGNED)) AS CHAR)
            WHERE REPLACE(IFNULL(COLUMN_03, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_3', v_log_after_rule3, NOW());

            /* Rule 4: [납입주기]≠"월납,일시납"이면 원수사 원부확인하여 [보험료] 값수정 및 [납입주기]="일시납"으로 값수정 */
            UPDATE T_TEMP_RPA_MRF_PROCESSED a
            INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
            ON
                a.COLUMN_02 = b.SEARCH_DATA
                AND b.SYS_FLAG = '1'
                AND b.BATCH_ID = IN_BATCH_ID
                AND b.COMPANY_CODE = v_company_code
                AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                AND b.BUSINESS_RULE_NO = 4
                AND b.COLUMN_NAME = '보험료'
                AND b.ACTION = 'UPD'
            SET a.COLUMN_03 = b.AFTER_COLUMN_DATA,
                a.COLUMN_04 = '일시납';

            /* Rule 5: [보험기간 시작일]을 원수사 원부확인하여 값입력 */
            UPDATE T_TEMP_RPA_MRF_PROCESSED a
            INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
            ON
                a.COLUMN_02 = b.SEARCH_DATA
                AND b.SYS_FLAG = '1'
                AND b.BATCH_ID = IN_BATCH_ID
                AND b.COMPANY_CODE = v_company_code
                AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                AND b.BUSINESS_RULE_NO = 5
                AND b.COLUMN_NAME = '보험기간 시작일'
                AND b.ACTION = 'ADD'
            SET a.COLUMN_40 = b.AFTER_COLUMN_DATA;

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'NEW' AND UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            -- Rule 1: 맨 마지막열 값 추가(2개)
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_38 = '년납',
                COLUMN_39 = DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2.1: 계약번호 오름차순 정렬
            SET @seq := 0;
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_02 ASC, EXCEL_ROW_INDEX ASC;

            -- Rule 2.2: 중복 계약번호의 [영수]=모두 "정상"면 해당 데이터들 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_del_case;
            CREATE TEMPORARY TABLE tmp_mrf_del_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_07 <> '정상' THEN 1 ELSE 0 END) = 0;

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_del_case);

            -- Rule 2.3: 중복 계약번호 중 [영수]=각각"신계약,배서"면 "배서" 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_del_case;
            CREATE TEMPORARY TABLE tmp_mrf_del_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING SUM(CASE WHEN COLUMN_07 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_07 = '배서' THEN 1 ELSE 0 END) > 0;

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_del_case)
              AND COLUMN_07 = '배서';

            -- Rule 2.4: 중복 계약번호 중 [영수]=각각"신계약,취소"면 [영수]="취소"로 수정 및 [보험료]="마이너스금액" 데이터 행삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mrf_dup_case;
            CREATE TEMPORARY TABLE tmp_mrf_dup_case
            SELECT COLUMN_02
            FROM T_TEMP_RPA_MRF_PROCESSED
            GROUP BY COLUMN_02
            HAVING SUM(CASE WHEN COLUMN_07 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_07 = '취소' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_MRF_PROCESSED t
            INNER JOIN tmp_mrf_dup_case d ON t.COLUMN_02 = d.COLUMN_02
            SET t.COLUMN_07 = '취소';

            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE COLUMN_02 IN (SELECT COLUMN_02 FROM tmp_mrf_dup_case)
              AND REPLACE(IFNULL(COLUMN_03, '0'), ',', '') REGEXP '^-[0-9]+';

            -- Rule 2.5: 청약일≠해당월 & [보험료]="마이너스금액" 데이터 행삭제
            DELETE FROM T_TEMP_RPA_MRF_PROCESSED
            WHERE LEFT(REPLACE(COLUMN_32, '-', ''), 6) <> DATE_FORMAT(CURDATE(), '%Y%m')
              AND REPLACE(IFNULL(COLUMN_03, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            /* Rule 3: [납입주기]≠월납,일시납이면 원수사 원부확인하여 [보험료] 값수정 및 [납입주기]="일시납"으로 값수정 */
            UPDATE T_TEMP_RPA_MRF_PROCESSED a
            INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
            ON
                a.COLUMN_02 = b.SEARCH_DATA
                AND b.SYS_FLAG = '1'
                AND b.BATCH_ID = IN_BATCH_ID
                AND b.COMPANY_CODE = v_company_code
                AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                AND b.BUSINESS_RULE_NO = 3
                AND b.COLUMN_NAME = '보험료'
                AND b.ACTION = 'UPD'
            SET a.COLUMN_03 = b.AFTER_COLUMN_DATA,
                a.COLUMN_04 = '일시납';

            /* Rule 4: [보험기간 시작일]을 원수사 원부확인하여 값입력 */
            UPDATE T_TEMP_RPA_MRF_PROCESSED a
            INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
            ON
                a.COLUMN_02 = b.SEARCH_DATA
                AND b.SYS_FLAG = '1'
                AND b.BATCH_ID = IN_BATCH_ID
                AND b.COMPANY_CODE = v_company_code
                AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                AND b.BUSINESS_RULE_NO = 4
                AND b.COLUMN_NAME = '보험기간 시작일'
                AND b.ACTION = 'ADD'
            SET a.COLUMN_40 = b.AFTER_COLUMN_DATA;

        ELSEIF UPPER(IN_CONTRACT_TYPE) = 'EXT' AND UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            -- Rule 1: [계약상태명]=“정상,해지,해지불능”이면 [소멸실효일자]를 “0000-00-00”으로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_54 = '0000-00-00'
            WHERE COLUMN_53 IN ('정상', '해지', '해지불능');

            SELECT COUNT(*) INTO v_log_after_rule1 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_1', v_log_after_rule1, NOW());

            -- Rule 2: [계약상세상태명]=“모든 완납, 모든 납입면제 제외” 후 [계약상태명]="정상"건만 추출하여 [최종납입년월] 연체건은 [계약상태명]값을 "연체"로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_53 = '연체'
            WHERE COLUMN_56 NOT LIKE '%완납%'
              AND COLUMN_56 NOT LIKE '%납입면제%'
              AND COLUMN_53 = '정상'
              AND COLUMN_08 < DATE_FORMAT(CURDATE(), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule2 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_2', v_log_after_rule2, NOW());

            -- Rule 3: [계약상세상태명]=“중지”이면, [계약상태명]값을 "정상"으로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_53 = '정상'
            WHERE COLUMN_56 = '중지';

            SELECT COUNT(*) INTO v_log_after_rule3 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_3', v_log_after_rule3, NOW());

            -- Rule 4: [청약일자],[보험개시일자],[보험종료일자]를 간단한날짜 서식으로 변경
            UPDATE IGNORE T_TEMP_RPA_MRF_PROCESSED
            SET 
                COLUMN_04 = DATE_FORMAT(STR_TO_DATE(COLUMN_04, '%m/%d/%Y'), '%Y%m%d'),
                COLUMN_06 = DATE_FORMAT(STR_TO_DATE(COLUMN_06, '%m/%d/%Y'), '%Y%m%d'),
                COLUMN_07 = DATE_FORMAT(STR_TO_DATE(COLUMN_07, '%m/%d/%Y'), '%Y%m%d')
            WHERE COLUMN_04 LIKE '%/%';

            SELECT COUNT(*) INTO v_log_after_rule4 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_4', v_log_after_rule4, NOW());

            -- Rule 5: [계약상세상태명]=“취소,철회”이면, [최종납입일자]="계약일자"로 값수정
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_55 = COLUMN_04
            WHERE COLUMN_56 IN ('취소', '철회');

            SELECT COUNT(*) INTO v_log_after_rule5 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_5', v_log_after_rule5, NOW());

            -- Rule 6: [계약상세상태명]=“실효” & [최종납입년월]=“실효 3년 경과”면, [계약상태명]값을 “시효”로 변경
            UPDATE T_TEMP_RPA_MRF_PROCESSED
            SET COLUMN_53 = '시효'
            WHERE COLUMN_56 = '실효'
              AND COLUMN_08 <= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 38 MONTH), '%Y%m');

            SELECT COUNT(*) INTO v_log_after_rule6 FROM T_TEMP_RPA_MRF_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'AFTER_RULE_6', v_log_after_rule6, NOW());
        END IF;

        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_MRF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, v_company_code, IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'FINAL_INSERT', v_row_count, NOW());

        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_MRF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_mrf_dup_case;
        DROP TEMPORARY TABLE IF EXISTS tmp_mrf_del_case;
    END IF;

END