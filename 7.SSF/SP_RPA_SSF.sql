/*
 * SP_RPA_SSF
 * Description : Process Samsung Fire insurance data
 * Parameters  :
 *   IN_BATCH_ID       : Batch ID to process
 *   IN_INSURANCE_TYPE : Insurance type (LTR / CAR / GEN)
 *   IN_CONTRACT_TYPE  : Contract type (NEW only)
 * Steps       :
 *   1. Hardcoded column mapping by insurance type (LTR / CAR / GEN)
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules (LTR / CAR / GEN)
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_SSF`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols        TEXT         DEFAULT '';
    DECLARE v_proc_cols       TEXT         DEFAULT '';
    DECLARE v_row_count       INT          DEFAULT 0;
    DECLARE v_company_code    VARCHAR(10)  DEFAULT 'SSF';
    DECLARE v_raw_table       VARCHAR(100) DEFAULT '';
    DECLARE v_processed_table VARCHAR(100) DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSF_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_sorted_ssf;
        DROP TEMPORARY TABLE IF EXISTS tmp_tae_a;
        DROP TEMPORARY TABLE IF EXISTS tmp_tae_agg;
        DROP TEMPORARY TABLE IF EXISTS tmp_blank;
        DROP TEMPORARY TABLE IF EXISTS tmp_blank_agg;
        DROP TEMPORARY TABLE IF EXISTS tmp_all_bseo;
        DROP TEMPORARY TABLE IF EXISTS tmp_mix_bseo;
        DROP TEMPORARY TABLE IF EXISTS tmp_tae_keep;
        DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_keep;
        
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_contract;
        DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_sum;

        DROP TEMPORARY TABLE IF EXISTS tmp_tae_contract;
        DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_dup;
    END;

    -- 1. Hardcoded Column Mapping
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';

        -- Mapping for LTR contracts (Columns 01-66 + Target-only 67, 68)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후 이관여부
            'COLUMN_39, '); -- RC계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후 이관여부
            'COLUMN_39, '); -- RC계약여부

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율

        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수

        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명

        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드

        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID

        -- 67-68 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols,
            'NULL, ', -- 납기구분
            'NULL');  -- 납입월
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_67, ', -- 납기구분
            'COLUMN_68');  -- 납입월

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
        SET v_raw_table = 'T_RPA_CAR_RAW';
        SET v_processed_table = 'T_RPA_CAR_PROCESSED';

        -- Mapping for CAR contracts (Columns 01-66 + Target-only 67)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후 이관여부
            'COLUMN_39, '); -- RC계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후 이관여부
            'COLUMN_39, '); -- RC계약여부

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율

        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수

        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명

        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드

        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID

        -- 67 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols, 'NULL');        -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_67'); -- 납기구분

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
        SET v_raw_table = 'T_RPA_GENERAL_RAW';
        SET v_processed_table = 'T_RPA_GENERAL_PROCESSED';

        -- Mapping for GEN contracts (Columns 01-66 + Target-only 67)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 계약번호
            'COLUMN_02, ', -- 상품명
            'COLUMN_03, '); -- 상품코드

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 계약자명
            'COLUMN_05, ', -- 피보험자명
            'COLUMN_06, '); -- 납입회차

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 대체계약여부
            'COLUMN_08, ', -- 보험료
            'COLUMN_09, '); -- 월납보험료

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 평가실적
            'COLUMN_11, ', -- 월납환산수정P
            'COLUMN_12, '); -- 책임보험료

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 임의보험료
            'COLUMN_14, ', -- 커미션계
            'COLUMN_15, '); -- 신계약/계약관리

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 선지급
            'COLUMN_17, ', -- 유지
            'COLUMN_18, '); -- 책임

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 임의
            'COLUMN_20, ', -- 순번
            'COLUMN_21, '); -- 담보패키지

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 계약상태
            'COLUMN_23, ', -- 정상집금여부
            'COLUMN_24, '); -- 장기청약일

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 보험시기
            'COLUMN_26, ', -- 보험종기
            'COLUMN_27, '); -- 마감일

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 영수일
            'COLUMN_29, ', -- 응당일
            'COLUMN_30, '); -- 보험기간

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 납입기간
            'COLUMN_32, ', -- 납입주기
            'COLUMN_33, '); -- 집금방법

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 장기갱신주기
            'COLUMN_35, ', -- 갱신회차
            'COLUMN_36, '); -- 수금성과구분

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후 이관여부
            'COLUMN_39, '); -- RC계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 본인/이관
            'COLUMN_38, ', -- 18년이후 이관여부
            'COLUMN_39, '); -- RC계약여부

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 장기상품구분
            'COLUMN_41, ', -- 보장新수정P
            'COLUMN_42, '); -- 보장P_수정P

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 자동차비사업/사업
            'COLUMN_44, ', -- 자동차갱신여부
            'COLUMN_45, '); -- AIP

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 차종
            'COLUMN_47, ', -- 성과인정여부
            'COLUMN_48, '); -- 차량번호

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 일반고정/변동
            'COLUMN_50, ', -- 출재율
            'COLUMN_51, '); -- 출재수수료율

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 예정사업비율
            'COLUMN_53, ', -- 재원한도(G/L)
            'COLUMN_54, '); -- 일반할인율

        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 글로벌여부
            'COLUMN_56, ', -- 브로커여부
            'COLUMN_57, '); -- 공동인수

        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_58, ', -- 일반마스터증번
            'COLUMN_59, ', -- 지사코드
            'COLUMN_60, '); -- 지사명

        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_61, ', -- 사용인코드
            'COLUMN_62, ', -- 사용인명
            'COLUMN_63, '); -- 모집자코드

        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_64, ', -- 모집사용인
            'COLUMN_65, ', -- 전환여부
            'COLUMN_66, '); -- Case_ID

        -- 67 (Target-only)
        SET v_raw_cols = CONCAT(v_raw_cols, 'NULL');        -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols, 'COLUMN_67'); -- 납기구분

    END IF;

    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSF_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_SSF_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_SSF_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 2.3. Apply transformation rules (LTR / CAR / GEN)

        -- [LTR Logic]
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            /* Rule 1: 맨 마지막열 값 추가(2개)
               ① 항목명I : 납기구분 / 항목값 : 년납
               ② 항목명II : 납입월 / 항목값 : 해당월(ex.202512)
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_67 = '년납',
                COLUMN_68 = DATE_FORMAT(CURDATE(), '%Y%m');

            /* Rule 2: [계약상태] 편집
               ① [계약상태]≠"신계약,취소,해지"이면 데이터 행삭제
               ② [계약상태]="해지,취소" & [장기청약일]≠해당월이면 데이터 행삭제
            */
            -- Rule 2①
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_22 NOT IN ('신계약', '취소', '해지');

            -- Rule 2②
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_22 IN ('해지', '취소')
              AND DATE_FORMAT(COLUMN_24, '%Y%m') != DATE_FORMAT(CURDATE(), '%Y%m');

           /* Rule 3: 계약번호 중복 편집
            ① 계약번호 오름차순 정렬
            ② 
            중복 계약번호 중 [피보험자]="태아"가 있는 경우 "보험료, 월납환산수정P"항목은 합산하여 한건으로 값수정

            (단, [피보험자명]="태아/계약자명"이면 "태아"로, [피보험자명]="공백, 계약자명"이면 "계약자명"으로 한건만 반영)
            */

            -- ① 계약번호 오름차순 정렬
            SET @seq := 0;
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_01 ASC, EXCEL_ROW_INDEX ASC;

            -- Find duplicated contracts
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_contract;
            CREATE TEMPORARY TABLE tmp_dup_contract
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            GROUP BY COLUMN_01
            HAVING COUNT(*) > 1;

            -- =====================================================
            -- Case 1: Duplicate contracts that HAVE '태아'
            -- Keep 1 row with 피보험자명='태아'
            -- Sum all duplicated rows into that row
            -- Delete all remaining rows
            -- =====================================================
            DROP TEMPORARY TABLE IF EXISTS tmp_tae_contract;
            CREATE TEMPORARY TABLE tmp_tae_contract
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_dup_contract)
            GROUP BY COLUMN_01
            HAVING SUM(CASE WHEN TRIM(IFNULL(COLUMN_05, '')) = '태아' THEN 1 ELSE 0 END) > 0;

            DROP TEMPORARY TABLE IF EXISTS tmp_tae_sum;
            CREATE TEMPORARY TABLE tmp_tae_sum
            SELECT
                COLUMN_01,
                CAST(SUM(CAST(REPLACE(IFNULL(COLUMN_08, '0'), ',', '') AS SIGNED)) AS CHAR) AS sum_08,
                CAST(SUM(CAST(REPLACE(IFNULL(COLUMN_11, '0'), ',', '') AS SIGNED)) AS CHAR) AS sum_11
            FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_tae_contract)
            GROUP BY COLUMN_01;

            DROP TEMPORARY TABLE IF EXISTS tmp_tae_keep;
            CREATE TEMPORARY TABLE tmp_tae_keep
            SELECT
                COLUMN_01,
                MIN(EXCEL_ROW_INDEX) AS keep_row
            FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_tae_contract)
            AND TRIM(IFNULL(COLUMN_05, '')) = '태아'
            GROUP BY COLUMN_01;

            UPDATE T_TEMP_RPA_SSF_PROCESSED t
            INNER JOIN tmp_tae_keep k
                ON t.COLUMN_01 = k.COLUMN_01
            AND t.EXCEL_ROW_INDEX = k.keep_row
            INNER JOIN tmp_tae_sum s
                ON t.COLUMN_01 = s.COLUMN_01
            SET
                t.COLUMN_08 = s.sum_08,
                t.COLUMN_11 = s.sum_11,
                t.COLUMN_05 = '태아';

            DELETE t
            FROM T_TEMP_RPA_SSF_PROCESSED t
            INNER JOIN tmp_tae_contract c
                ON t.COLUMN_01 = c.COLUMN_01
            LEFT JOIN tmp_tae_keep k
                ON t.COLUMN_01 = k.COLUMN_01
            AND t.EXCEL_ROW_INDEX = k.keep_row
            WHERE k.keep_row IS NULL;

            -- =====================================================
            -- Case 2: Duplicate contracts that DO NOT have '태아'
            -- and have both blank + non-blank 피보험자명
            -- Keep 1 non-blank row
            -- Sum all duplicated rows into that row
            -- Delete blank rows only
            -- If all rows are non-blank, do nothing
            -- =====================================================
            DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_mix;
            CREATE TEMPORARY TABLE tmp_no_tae_mix
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_dup_contract)
            GROUP BY COLUMN_01
            HAVING SUM(CASE WHEN TRIM(IFNULL(COLUMN_05, '')) = '태아' THEN 1 ELSE 0 END) = 0
            AND SUM(CASE WHEN TRIM(IFNULL(COLUMN_05, '')) = '' THEN 1 ELSE 0 END) > 0
            AND SUM(CASE WHEN TRIM(IFNULL(COLUMN_05, '')) <> '' THEN 1 ELSE 0 END) > 0;

            DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_sum;
            CREATE TEMPORARY TABLE tmp_no_tae_sum
            SELECT
                COLUMN_01,
                CAST(SUM(CAST(REPLACE(IFNULL(COLUMN_08, '0'), ',', '') AS SIGNED)) AS CHAR) AS sum_08,
                CAST(SUM(CAST(REPLACE(IFNULL(COLUMN_11, '0'), ',', '') AS SIGNED)) AS CHAR) AS sum_11
            FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_no_tae_mix)
            GROUP BY COLUMN_01;

            DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_keep;
            CREATE TEMPORARY TABLE tmp_no_tae_keep
            SELECT
                COLUMN_01,
                MIN(CASE WHEN TRIM(IFNULL(COLUMN_05, '')) <> '' THEN EXCEL_ROW_INDEX END) AS keep_row
            FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_no_tae_mix)
            GROUP BY COLUMN_01;

            UPDATE T_TEMP_RPA_SSF_PROCESSED t
            INNER JOIN tmp_no_tae_keep k
                ON t.COLUMN_01 = k.COLUMN_01
            AND t.EXCEL_ROW_INDEX = k.keep_row
            INNER JOIN tmp_no_tae_sum s
                ON t.COLUMN_01 = s.COLUMN_01
            SET
                t.COLUMN_08 = s.sum_08,
                t.COLUMN_11 = s.sum_11;

            DELETE t
            FROM T_TEMP_RPA_SSF_PROCESSED t
            INNER JOIN tmp_no_tae_mix d
                ON t.COLUMN_01 = d.COLUMN_01
            WHERE TRIM(IFNULL(t.COLUMN_05, '')) = '';

            -- Re-sequence after deletion
            SET @seq := 0;
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_01 ASC, EXCEL_ROW_INDEX ASC;

            -- Cleanup
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_contract;
            DROP TEMPORARY TABLE IF EXISTS tmp_tae_contract;
            DROP TEMPORARY TABLE IF EXISTS tmp_tae_sum;
            DROP TEMPORARY TABLE IF EXISTS tmp_tae_keep;
            DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_mix;
            DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_sum;
            DROP TEMPORARY TABLE IF EXISTS tmp_no_tae_keep;

            /* Rule 4: 상품명 원수사 원부확인하여 값수정
               (장기>계약상세조회>"특성조회항목" → 상품명 확인)
            */
            UPDATE T_TEMP_RPA_SSF_PROCESSED a
            INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
            ON
                a.COLUMN_02 = b.BEFORE_COLUMN_DATA
                AND b.SYS_FLAG = '1'
                AND b.BATCH_ID = IN_BATCH_ID
                AND b.COMPANY_CODE = v_company_code
                AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                AND b.BUSINESS_RULE_NO = 4
                AND b.COLUMN_NAME = '상품명'
                AND b.ACTION = 'UPD'
            SET a.COLUMN_02 = b.AFTER_COLUMN_DATA;

            /* Rule 5: [납입기간]="0"이면 원수사 원부확인하여 값수정
               → 장기>계약상세조회>"납입정보의 전체 회 / 12"계산한 값
            */
            UPDATE T_TEMP_RPA_SSF_PROCESSED a
            INNER JOIN T_RPA_INSURANCE_EXTRA_GUIDE b
            ON
                a.COLUMN_01 = b.SEARCH_DATA
                AND b.SYS_FLAG = '1'
                AND b.BATCH_ID = IN_BATCH_ID
                AND b.COMPANY_CODE = v_company_code
                AND b.INSURANCE_TYPE = IN_INSURANCE_TYPE
                AND b.CONTRACT_TYPE = IN_CONTRACT_TYPE
                AND b.BUSINESS_RULE_NO = 5
                AND b.COLUMN_NAME = '납입기간'
                AND b.ACTION = 'UPD'
            SET a.COLUMN_31 = b.AFTER_COLUMN_DATA;

        -- [CAR Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            /* Rule 1: 맨 마지막열 값 추가(1개)
               ① 항목명I : 납기구분 / 항목값 : 년납
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_67 = '년납';

            /* Rule 2: [계약상태]="배서"면, 데이터 행삭제 */
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_22 = '배서';

            /* Rule 3: [계약상태]="공란"이면 [계약상태]="신계약"으로 값수정 */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_22 = '신계약'
            WHERE COLUMN_22 IS NULL OR COLUMN_22 = '';

            /* Rule 4: 계약번호 중복 편집
               ① 계약번호 오름차순 정렬
               ② 중복 계약번호 중 [계약상태]=모두 "배서"면 해당 데이터들 행삭제
               ③ 중복 계약번호 중 [계약상태]=각각"신계약,배서"면 "배서" 데이터 행삭제
            */

            -- Rule 4①: 계약번호 오름차순 정렬
            SET @seq := 0;
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_01 ASC;

            -- Rule 4②: 중복 계약번호 중 모두 "배서"면 전체 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_all_bseo;
            CREATE TEMPORARY TABLE tmp_all_bseo
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            GROUP BY COLUMN_01
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_22 != '배서' THEN 1 ELSE 0 END) = 0;

            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_all_bseo);

            DROP TEMPORARY TABLE IF EXISTS tmp_all_bseo;

            -- Rule 4③: 중복 계약번호 중 "신계약,배서" 혼재면 "배서" 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mix_bseo;
            CREATE TEMPORARY TABLE tmp_mix_bseo
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            GROUP BY COLUMN_01
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_22 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_22 = '배서' THEN 1 ELSE 0 END) > 0;

            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_mix_bseo)
              AND COLUMN_22 = '배서';

            DROP TEMPORARY TABLE IF EXISTS tmp_mix_bseo;

            /* Rule 5: [보험료]="마이너스"이면 "플러스"값으로 수정 */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_08 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_08, '0'), ',', '') AS SIGNED)) AS CHAR)
            WHERE COLUMN_08 LIKE '-%';

        -- [GEN Logic]
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN

            /* Rule 1: 맨 마지막열 값 추가(1개)
               ① 항목명I : 납기구분 / 항목값 : 년납
               ※ 전체 행에 반영
            */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_67 = '년납';

            /* Rule 2: [계약상태]="배서"면 데이터 행삭제 */
            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_22 = '배서';

            /* Rule 3: [계약상태]="공란"이면 "신계약"으로 수정 */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_22 = '신계약'
            WHERE COLUMN_22 IS NULL OR COLUMN_22 = '';

            /* Rule 4: 계약번호 중복 편집
               ① 계약번호 오름차순 정렬
               ② 중복 계약번호 중 [계약상태]=모두 "배서"면 해당 데이터들 행삭제
               ③ 중복 계약번호 중 [계약상태]=각각"신계약,배서"면 "배서" 데이터 행삭제
            */

            -- Rule 4①: 계약번호 오름차순 정렬
            SET @seq := 0;
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET SORT_ORDER_NO = (@seq := @seq + 1)
            ORDER BY COLUMN_01 ASC;

            -- Rule 4②: 중복 계약번호 중 모두 "배서"면 전체 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_all_bseo;
            CREATE TEMPORARY TABLE tmp_all_bseo
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            GROUP BY COLUMN_01
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_22 != '배서' THEN 1 ELSE 0 END) = 0;

            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_all_bseo);

            DROP TEMPORARY TABLE IF EXISTS tmp_all_bseo;

            -- Rule 4③: 중복 계약번호 중 "신계약,배서" 혼재면 "배서" 삭제
            DROP TEMPORARY TABLE IF EXISTS tmp_mix_bseo;
            CREATE TEMPORARY TABLE tmp_mix_bseo
            SELECT COLUMN_01
            FROM T_TEMP_RPA_SSF_PROCESSED
            GROUP BY COLUMN_01
            HAVING COUNT(*) > 1
               AND SUM(CASE WHEN COLUMN_22 = '신계약' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_22 = '배서' THEN 1 ELSE 0 END) > 0;

            DELETE FROM T_TEMP_RPA_SSF_PROCESSED
            WHERE COLUMN_01 IN (SELECT COLUMN_01 FROM tmp_mix_bseo)
              AND COLUMN_22 = '배서';

            DROP TEMPORARY TABLE IF EXISTS tmp_mix_bseo;

            /* Rule 5: [보험료]="마이너스"이면 "플러스"값으로 수정 */
            UPDATE T_TEMP_RPA_SSF_PROCESSED
            SET COLUMN_08 = CAST(ABS(CAST(REPLACE(IFNULL(COLUMN_08, '0'), ',', '') AS SIGNED)) AS CHAR)
            WHERE COLUMN_08 LIKE '-%';

        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_SSF_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_SSF_PROCESSED;

    END IF;

END