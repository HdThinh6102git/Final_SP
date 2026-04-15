/*
 * SP_RPA_LTG
 * Description : Process Lotte Fire insurance data
 * Parameters  :
 *   IN_BATCH_ID       : Batch ID to process
 *   IN_INSURANCE_TYPE : Insurance type (LTR / CAR / GEN)
 *   IN_CONTRACT_TYPE  : Contract type (NEW / EXT)
 * Steps       :
 *   1. Hardcoded column mapping by insurance type / contract type
 *   2. Execute if column mapping is valid
 *      2.1. Create temp table
 *      2.2. Insert raw data into temp table
 *      2.3. Apply transformation rules (LTR / CAR / GEN / EXT)
 *      2.4. Insert transformed data into processed table
 *      2.5. Drop temp table
 */

CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_LTG`(
    IN IN_BATCH_ID       VARCHAR(100),
    IN IN_INSURANCE_TYPE VARCHAR(50),
    IN IN_CONTRACT_TYPE  VARCHAR(20)
)
BEGIN
    -- [DECLARE variables]
    DECLARE v_raw_cols         TEXT         DEFAULT '';
    DECLARE v_proc_cols        TEXT         DEFAULT '';
    DECLARE v_row_count        INT          DEFAULT 0;
    DECLARE v_company_code     VARCHAR(10)  DEFAULT 'LTG';
    DECLARE v_raw_table        VARCHAR(100) DEFAULT '';
    DECLARE v_processed_table  VARCHAR(100) DEFAULT '';
    DECLARE v_current_ym       VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym        VARCHAR(6)   DEFAULT '';

    -- [DECLARE debug variables]
    DECLARE v_log_initial_raw  INT DEFAULT 0;
    DECLARE v_log_temp_initial INT DEFAULT 0;
    DECLARE v_log_after_rule1  INT DEFAULT 0;
    DECLARE v_log_after_rule2  INT DEFAULT 0;
    DECLARE v_log_after_rule3  INT DEFAULT 0;
    DECLARE v_log_after_rule4  INT DEFAULT 0;

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @v_err_no = MYSQL_ERRNO,
            @v_err_msg = MESSAGE_TEXT;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (
            IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
            CONCAT('SQL_EXCEPTION: [', @v_err_no, '] ', @v_err_msg),
            0, NOW()
        );
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_ltg_dup_case;
    END;

    -- [SET logic]
    SET v_current_ym = DATE_FORMAT(CURDATE(), '%Y%m');
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 38 MONTH), '%Y%m');

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
    IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';

        -- Mapping for NEW LTR contracts (Columns 01-109 + Target-only 110)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 부문코드
            'COLUMN_02, ', -- 지역단코드
            'COLUMN_03, '); -- 지역단명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 부문코드
            'COLUMN_02, ', -- 지역단코드
            'COLUMN_03, '); -- 지역단명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 지점코드
            'COLUMN_05, ', -- 지점명
            'COLUMN_06, '); -- 취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 지점코드
            'COLUMN_05, ', -- 지점명
            'COLUMN_06, '); -- 취급자코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 취급자명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 출장소(지사코드)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 취급자명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 출장소(지사코드)

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 팀코드(지사명)
            'COLUMN_11, ', -- 관리사원(관리사원관리지점)
            'COLUMN_12, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 팀코드(지사명)
            'COLUMN_11, ', -- 관리사원(관리사원관리지점)
            'COLUMN_12, '); -- 모집자코드

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 모집자명
            'COLUMN_14, ', -- 보험구분
            'COLUMN_15, '); -- 보종코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 모집자명
            'COLUMN_14, ', -- 보험구분
            'COLUMN_15, '); -- 보종코드

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 보종명
            'COLUMN_17, ', -- 실적일자
            'COLUMN_18, '); -- 보험시기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 보종명
            'COLUMN_17, ', -- 실적일자
            'COLUMN_18, '); -- 보험시기

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보험종기
            'COLUMN_20, ', -- 수납실적일자
            'COLUMN_21, '); -- 주민번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보험종기
            'COLUMN_20, ', -- 수납실적일자
            'COLUMN_21, '); -- 주민번호

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 보험계약자명
            'COLUMN_23, ', -- 처리구분
            'COLUMN_24, '); -- 실적건수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 보험계약자명
            'COLUMN_23, ', -- 처리구분
            'COLUMN_24, '); -- 실적건수

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 가계구분
            'COLUMN_26, ', -- 수금방법
            'COLUMN_27, '); -- 금종구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 가계구분
            'COLUMN_26, ', -- 수금방법
            'COLUMN_27, '); -- 금종구분

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 수당응당월
            'COLUMN_29, ', -- 납입회차
            'COLUMN_30, '); -- 당사보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 수당응당월
            'COLUMN_29, ', -- 납입회차
            'COLUMN_30, '); -- 당사보험료

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 신계약수정보험료
            'COLUMN_32, ', -- 수금수정보험료
            'COLUMN_33, '); -- 수당합계
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 신계약수정보험료
            'COLUMN_32, ', -- 수금수정보험료
            'COLUMN_33, '); -- 수당합계

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 원수실적
            'COLUMN_35, ', -- 수입실적
            'COLUMN_36, '); -- 수납실적
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 원수실적
            'COLUMN_35, ', -- 수입실적
            'COLUMN_36, '); -- 수납실적

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 수당지급여부
            'COLUMN_38, ', -- 입금상태
            'COLUMN_39, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 수당지급여부
            'COLUMN_38, ', -- 입금상태
            'COLUMN_39, '); -- 납입방법

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 종구분
            'COLUMN_41, ', -- 군구분
            'COLUMN_42, '); -- 계약상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 종구분
            'COLUMN_41, ', -- 군구분
            'COLUMN_42, '); -- 계약상태

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 차량구분
            'COLUMN_44, ', -- 사업/비사업
            'COLUMN_45, '); -- 차종
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 차량구분
            'COLUMN_44, ', -- 사업/비사업
            'COLUMN_45, '); -- 차종

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 차량번호
            'COLUMN_47, ', -- 전계약사
            'COLUMN_48, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 차량번호
            'COLUMN_47, ', -- 전계약사
            'COLUMN_48, '); -- 납입기간

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 물건
            'COLUMN_50, ', -- 공동인수여부
            'COLUMN_51, '); -- 사용인
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 물건
            'COLUMN_50, ', -- 공동인수여부
            'COLUMN_51, '); -- 사용인

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 사용인명
            'COLUMN_53, ', -- 미수여부
            'COLUMN_54, '); -- 장기신규(자동이체여부)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 사용인명
            'COLUMN_53, ', -- 미수여부
            'COLUMN_54, '); -- 장기신규(자동이체여부)

        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 자동이체 일자
            'COLUMN_56, ', -- 동일증권유무
            'COLUMN_57, '); -- 보종구분값
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 자동이체 일자
            'COLUMN_56, ', -- 동일증권유무
            'COLUMN_57, '); -- 보종구분값

        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_58, ', -- 전자서명여부
            'COLUMN_59, ', -- 자기계약여부
            'COLUMN_60, '); -- 승환계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_58, ', -- 전자서명여부
            'COLUMN_59, ', -- 자기계약여부
            'COLUMN_60, '); -- 승환계약여부

        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_61, ', -- 승환계약상태명
            'COLUMN_62, ', -- 자동차(신/타)
            'COLUMN_63, '); -- 일반실적그룹
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_61, ', -- 승환계약상태명
            'COLUMN_62, ', -- 자동차(신/타)
            'COLUMN_63, '); -- 일반실적그룹

        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_64, ', -- 일반실적구분
            'COLUMN_65, ', -- 일반실적구분코드
            'COLUMN_66, '); -- 전자서명일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_64, ', -- 일반실적구분
            'COLUMN_65, ', -- 일반실적구분코드
            'COLUMN_66, '); -- 전자서명일자

        -- 67-69
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_67, ', -- 전자서명시간
            'COLUMN_68, ', -- 모대리점코드
            'COLUMN_69, '); -- 모대리점명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_67, ', -- 전자서명시간
            'COLUMN_68, ', -- 모대리점코드
            'COLUMN_69, '); -- 모대리점명

        -- 70-72
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_70, ', -- 모대리점 관리본부
            'COLUMN_71, ', -- 장기할증전보험료
            'COLUMN_72, '); -- 보장보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_70, ', -- 모대리점 관리본부
            'COLUMN_71, ', -- 장기할증전보험료
            'COLUMN_72, '); -- 보장보험료

        -- 73-75
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_73, ', -- 적립보험료
            'COLUMN_74, ', -- 설계번호
            'COLUMN_75, '); -- 피보험자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_73, ', -- 적립보험료
            'COLUMN_74, ', -- 설계번호
            'COLUMN_75, '); -- 피보험자명

        -- 76-78
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_76, ', -- 물건명1
            'COLUMN_77, ', -- 물건명2
            'COLUMN_78, '); -- 물건명3
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_76, ', -- 물건명1
            'COLUMN_77, ', -- 물건명2
            'COLUMN_78, '); -- 물건명3

        -- 79-81
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_79, ', -- 플랜
            'COLUMN_80, ', -- 중개인
            'COLUMN_81, '); -- 중개인명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_79, ', -- 플랜
            'COLUMN_80, ', -- 중개인
            'COLUMN_81, '); -- 중개인명

        -- 82-84
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_82, ', -- 설계지원최초취급자코드
            'COLUMN_83, ', -- 설계지원최초취급자명
            'COLUMN_84, '); -- 설계지원최종취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_82, ', -- 설계지원최초취급자코드
            'COLUMN_83, ', -- 설계지원최초취급자명
            'COLUMN_84, '); -- 설계지원최종취급자코드

        -- 85-87
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_85, ', -- 설계지원최종취급자명
            'COLUMN_86, ', -- 출생후계속보험료
            'COLUMN_87, '); -- 수입보험료입력일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_85, ', -- 설계지원최종취급자명
            'COLUMN_86, ', -- 출생후계속보험료
            'COLUMN_87, '); -- 수입보험료입력일자

        -- 88-90
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_88, ', -- 유의계약여부
            'COLUMN_89, ', -- 최초취급자코드(계약자주민번호기준)
            'COLUMN_90, '); -- 최조취급자명(계약자주민번호기준)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_88, ', -- 유의계약여부
            'COLUMN_89, ', -- 최초취급자코드(계약자주민번호기준)
            'COLUMN_90, '); -- 최조취급자명(계약자주민번호기준)

        -- 91-93
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_91, ', -- 할인전 보험료
            'COLUMN_92, ', -- 마케팅활용동의여부
            'COLUMN_93, '); -- 계약자고객ID
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_91, ', -- 할인전 보험료
            'COLUMN_92, ', -- 마케팅활용동의여부
            'COLUMN_93, '); -- 계약자고객ID

        -- 94-96
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_94, ', -- 일반무해지구분
            'COLUMN_95, ', -- 청약일시
            'COLUMN_96, '); -- 실손전환전갱신주기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_94, ', -- 일반무해지구분
            'COLUMN_95, ', -- 청약일시
            'COLUMN_96, '); -- 실손전환전갱신주기

        -- 97-99
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_97, ', -- 취소일자
            'COLUMN_98, ', -- 지급사유코드명
            'COLUMN_99, '); -- 크로스계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_97, ', -- 취소일자
            'COLUMN_98, ', -- 지급사유코드명
            'COLUMN_99, '); -- 크로스계약여부

        -- 100-102
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_100, ', -- 당사승환회차
            'COLUMN_101, ', -- 피보험자수
            'COLUMN_102, '); -- 보장보험료구성비
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_100, ', -- 당사승환회차
            'COLUMN_101, ', -- 피보험자수
            'COLUMN_102, '); -- 보장보험료구성비

        -- 103-105
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_103, ', -- 팀명
            'COLUMN_104, ', -- w설계시작여부
            'COLUMN_105, '); -- w설계활용여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_103, ', -- 팀명
            'COLUMN_104, ', -- w설계시작여부
            'COLUMN_105, '); -- w설계활용여부

        -- 106-108
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_106, ', -- EtoE여부
            'COLUMN_107, ', -- 차량등급코드
            'COLUMN_108, '); -- 권유직원
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_106, ', -- EtoE여부
            'COLUMN_107, ', -- 차량등급코드
            'COLUMN_108, '); -- 권유직원

        -- 109-110
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_109, ', -- 피보험자고객ID
            'NULL');        -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_109, ', -- 피보험자고객ID
            'COLUMN_110');  -- 납기구분

    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        SET v_raw_table = 'T_RPA_CAR_RAW';
        SET v_processed_table = 'T_RPA_CAR_PROCESSED';

        -- Mapping for NEW CAR contracts (Columns 01-109 + Target-only 110)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 부문코드
            'COLUMN_02, ', -- 지역단코드
            'COLUMN_03, '); -- 지역단명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 부문코드
            'COLUMN_02, ', -- 지역단코드
            'COLUMN_03, '); -- 지역단명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 지점코드
            'COLUMN_05, ', -- 지점명
            'COLUMN_06, '); -- 취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 지점코드
            'COLUMN_05, ', -- 지점명
            'COLUMN_06, '); -- 취급자코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 취급자명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 출장소(지사코드)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 취급자명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 출장소(지사코드)

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 팀코드(지사명)
            'COLUMN_11, ', -- 관리사원(관리사원관리지점)
            'COLUMN_12, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 팀코드(지사명)
            'COLUMN_11, ', -- 관리사원(관리사원관리지점)
            'COLUMN_12, '); -- 모집자코드

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 모집자명
            'COLUMN_14, ', -- 보험구분
            'COLUMN_15, '); -- 보종코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 모집자명
            'COLUMN_14, ', -- 보험구분
            'COLUMN_15, '); -- 보종코드

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 보종명
            'COLUMN_17, ', -- 실적일자
            'COLUMN_18, '); -- 보험시기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 보종명
            'COLUMN_17, ', -- 실적일자
            'COLUMN_18, '); -- 보험시기

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보험종기
            'COLUMN_20, ', -- 수납실적일자
            'COLUMN_21, '); -- 주민번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보험종기
            'COLUMN_20, ', -- 수납실적일자
            'COLUMN_21, '); -- 주민번호

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 보험계약자명
            'COLUMN_23, ', -- 처리구분
            'COLUMN_24, '); -- 실적건수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 보험계약자명
            'COLUMN_23, ', -- 처리구분
            'COLUMN_24, '); -- 실적건수

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 가계구분
            'COLUMN_26, ', -- 수금방법
            'COLUMN_27, '); -- 금종구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 가계구분
            'COLUMN_26, ', -- 수금방법
            'COLUMN_27, '); -- 금종구분

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 수당응당월
            'COLUMN_29, ', -- 납입회차
            'COLUMN_30, '); -- 당사보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 수당응당월
            'COLUMN_29, ', -- 납입회차
            'COLUMN_30, '); -- 당사보험료

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 신계약수정보험료
            'COLUMN_32, ', -- 수금수정보험료
            'COLUMN_33, '); -- 수당합계
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 신계약수정보험료
            'COLUMN_32, ', -- 수금수정보험료
            'COLUMN_33, '); -- 수당합계

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 원수실적
            'COLUMN_35, ', -- 수입실적
            'COLUMN_36, '); -- 수납실적
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 원수실적
            'COLUMN_35, ', -- 수입실적
            'COLUMN_36, '); -- 수납실적

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 수당지급여부
            'COLUMN_38, ', -- 입금상태
            'COLUMN_39, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 수당지급여부
            'COLUMN_38, ', -- 입금상태
            'COLUMN_39, '); -- 납입방법

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 종구분
            'COLUMN_41, ', -- 군구분
            'COLUMN_42, '); -- 계약상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 종구분
            'COLUMN_41, ', -- 군구분
            'COLUMN_42, '); -- 계약상태

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 차량구분
            'COLUMN_44, ', -- 사업/비사업
            'COLUMN_45, '); -- 차종
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 차량구분
            'COLUMN_44, ', -- 사업/비사업
            'COLUMN_45, '); -- 차종

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 차량번호
            'COLUMN_47, ', -- 전계약사
            'COLUMN_48, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 차량번호
            'COLUMN_47, ', -- 전계약사
            'COLUMN_48, '); -- 납입기간

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 물건
            'COLUMN_50, ', -- 공동인수여부
            'COLUMN_51, '); -- 사용인
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 물건
            'COLUMN_50, ', -- 공동인수여부
            'COLUMN_51, '); -- 사용인

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 사용인명
            'COLUMN_53, ', -- 미수여부
            'COLUMN_54, '); -- 장기신규(자동이체여부)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 사용인명
            'COLUMN_53, ', -- 미수여부
            'COLUMN_54, '); -- 장기신규(자동이체여부)

        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 자동이체 일자
            'COLUMN_56, ', -- 동일증권유무
            'COLUMN_57, '); -- 보종구분값
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 자동이체 일자
            'COLUMN_56, ', -- 동일증권유무
            'COLUMN_57, '); -- 보종구분값

        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_58, ', -- 전자서명여부
            'COLUMN_59, ', -- 자기계약여부
            'COLUMN_60, '); -- 승환계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_58, ', -- 전자서명여부
            'COLUMN_59, ', -- 자기계약여부
            'COLUMN_60, '); -- 승환계약여부

        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_61, ', -- 승환계약상태명
            'COLUMN_62, ', -- 자동차(신/타)
            'COLUMN_63, '); -- 일반실적그룹
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_61, ', -- 승환계약상태명
            'COLUMN_62, ', -- 자동차(신/타)
            'COLUMN_63, '); -- 일반실적그룹

        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_64, ', -- 일반실적구분
            'COLUMN_65, ', -- 일반실적구분코드
            'COLUMN_66, '); -- 전자서명일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_64, ', -- 일반실적구분
            'COLUMN_65, ', -- 일반실적구분코드
            'COLUMN_66, '); -- 전자서명일자

        -- 67-69
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_67, ', -- 전자서명시간
            'COLUMN_68, ', -- 모대리점코드
            'COLUMN_69, '); -- 모대리점명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_67, ', -- 전자서명시간
            'COLUMN_68, ', -- 모대리점코드
            'COLUMN_69, '); -- 모대리점명

        -- 70-72
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_70, ', -- 모대리점 관리본부
            'COLUMN_71, ', -- 장기할증전보험료
            'COLUMN_72, '); -- 보장보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_70, ', -- 모대리점 관리본부
            'COLUMN_71, ', -- 장기할증전보험료
            'COLUMN_72, '); -- 보장보험료

        -- 73-75
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_73, ', -- 적립보험료
            'COLUMN_74, ', -- 설계번호
            'COLUMN_75, '); -- 피보험자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_73, ', -- 적립보험료
            'COLUMN_74, ', -- 설계번호
            'COLUMN_75, '); -- 피보험자명

        -- 76-78
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_76, ', -- 물건명1
            'COLUMN_77, ', -- 물건명2
            'COLUMN_78, '); -- 물건명3
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_76, ', -- 물건명1
            'COLUMN_77, ', -- 물건명2
            'COLUMN_78, '); -- 물건명3

        -- 79-81
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_79, ', -- 플랜
            'COLUMN_80, ', -- 중개인
            'COLUMN_81, '); -- 중개인명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_79, ', -- 플랜
            'COLUMN_80, ', -- 중개인
            'COLUMN_81, '); -- 중개인명

        -- 82-84
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_82, ', -- 설계지원최초취급자코드
            'COLUMN_83, ', -- 설계지원최초취급자명
            'COLUMN_84, '); -- 설계지원최종취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_82, ', -- 설계지원최초취급자코드
            'COLUMN_83, ', -- 설계지원최초취급자명
            'COLUMN_84, '); -- 설계지원최종취급자코드

        -- 85-87
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_85, ', -- 설계지원최종취급자명
            'COLUMN_86, ', -- 출생후계속보험료
            'COLUMN_87, '); -- 수입보험료입력일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_85, ', -- 설계지원최종취급자명
            'COLUMN_86, ', -- 출생후계속보험료
            'COLUMN_87, '); -- 수입보험료입력일자

        -- 88-90
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_88, ', -- 유의계약여부
            'COLUMN_89, ', -- 최초취급자코드(계약자주민번호기준)
            'COLUMN_90, '); -- 최조취급자명(계약자주민번호기준)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_88, ', -- 유의계약여부
            'COLUMN_89, ', -- 최초취급자코드(계약자주민번호기준)
            'COLUMN_90, '); -- 최조취급자명(계약자주민번호기준)

        -- 91-93
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_91, ', -- 할인전 보험료
            'COLUMN_92, ', -- 마케팅활용동의여부
            'COLUMN_93, '); -- 계약자고객ID
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_91, ', -- 할인전 보험료
            'COLUMN_92, ', -- 마케팅활용동의여부
            'COLUMN_93, '); -- 계약자고객ID

        -- 94-96
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_94, ', -- 일반무해지구분
            'COLUMN_95, ', -- 청약일시
            'COLUMN_96, '); -- 실손전환전갱신주기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_94, ', -- 일반무해지구분
            'COLUMN_95, ', -- 청약일시
            'COLUMN_96, '); -- 실손전환전갱신주기

        -- 97-99
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_97, ', -- 취소일자
            'COLUMN_98, ', -- 지급사유코드명
            'COLUMN_99, '); -- 크로스계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_97, ', -- 취소일자
            'COLUMN_98, ', -- 지급사유코드명
            'COLUMN_99, '); -- 크로스계약여부

        -- 100-102
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_100, ', -- 당사승환회차
            'COLUMN_101, ', -- 피보험자수
            'COLUMN_102, '); -- 보장보험료구성비
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_100, ', -- 당사승환회차
            'COLUMN_101, ', -- 피보험자수
            'COLUMN_102, '); -- 보장보험료구성비

        -- 103-105
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_103, ', -- 팀명
            'COLUMN_104, ', -- w설계시작여부
            'COLUMN_105, '); -- w설계활용여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_103, ', -- 팀명
            'COLUMN_104, ', -- w설계시작여부
            'COLUMN_105, '); -- w설계활용여부

        -- 106-108
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_106, ', -- EtoE여부
            'COLUMN_107, ', -- 차량등급코드
            'COLUMN_108, '); -- 권유직원
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_106, ', -- EtoE여부
            'COLUMN_107, ', -- 차량등급코드
            'COLUMN_108, '); -- 권유직원

        -- 109-110
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_109, ', -- 피보험자고객ID
            'NULL');        -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_109, ', -- 피보험자고객ID
            'COLUMN_110');  -- 납기구분
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        SET v_raw_table = 'T_RPA_GENERAL_RAW';
        SET v_processed_table = 'T_RPA_GENERAL_PROCESSED';

        -- Mapping for NEW GEN contracts (Columns 01-109 + Target-only 110)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 부문코드
            'COLUMN_02, ', -- 지역단코드
            'COLUMN_03, '); -- 지역단명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 부문코드
            'COLUMN_02, ', -- 지역단코드
            'COLUMN_03, '); -- 지역단명

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 지점코드
            'COLUMN_05, ', -- 지점명
            'COLUMN_06, '); -- 취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 지점코드
            'COLUMN_05, ', -- 지점명
            'COLUMN_06, '); -- 취급자코드

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 취급자명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 출장소(지사코드)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 취급자명
            'COLUMN_08, ', -- 증권번호
            'COLUMN_09, '); -- 출장소(지사코드)

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 팀코드(지사명)
            'COLUMN_11, ', -- 관리사원(관리사원관리지점)
            'COLUMN_12, '); -- 모집자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 팀코드(지사명)
            'COLUMN_11, ', -- 관리사원(관리사원관리지점)
            'COLUMN_12, '); -- 모집자코드

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 모집자명
            'COLUMN_14, ', -- 보험구분
            'COLUMN_15, '); -- 보종코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 모집자명
            'COLUMN_14, ', -- 보험구분
            'COLUMN_15, '); -- 보종코드

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 보종명
            'COLUMN_17, ', -- 실적일자
            'COLUMN_18, '); -- 보험시기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 보종명
            'COLUMN_17, ', -- 실적일자
            'COLUMN_18, '); -- 보험시기

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 보험종기
            'COLUMN_20, ', -- 수납실적일자
            'COLUMN_21, '); -- 주민번호
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 보험종기
            'COLUMN_20, ', -- 수납실적일자
            'COLUMN_21, '); -- 주민번호

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 보험계약자명
            'COLUMN_23, ', -- 처리구분
            'COLUMN_24, '); -- 실적건수
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 보험계약자명
            'COLUMN_23, ', -- 처리구분
            'COLUMN_24, '); -- 실적건수

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 가계구분
            'COLUMN_26, ', -- 수금방법
            'COLUMN_27, '); -- 금종구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 가계구분
            'COLUMN_26, ', -- 수금방법
            'COLUMN_27, '); -- 금종구분

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 수당응당월
            'COLUMN_29, ', -- 납입회차
            'COLUMN_30, '); -- 당사보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 수당응당월
            'COLUMN_29, ', -- 납입회차
            'COLUMN_30, '); -- 당사보험료

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 신계약수정보험료
            'COLUMN_32, ', -- 수금수정보험료
            'COLUMN_33, '); -- 수당합계
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 신계약수정보험료
            'COLUMN_32, ', -- 수금수정보험료
            'COLUMN_33, '); -- 수당합계

        -- 34-36
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34, ', -- 원수실적
            'COLUMN_35, ', -- 수입실적
            'COLUMN_36, '); -- 수납실적
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34, ', -- 원수실적
            'COLUMN_35, ', -- 수입실적
            'COLUMN_36, '); -- 수납실적

        -- 37-39
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_37, ', -- 수당지급여부
            'COLUMN_38, ', -- 입금상태
            'COLUMN_39, '); -- 납입방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_37, ', -- 수당지급여부
            'COLUMN_38, ', -- 입금상태
            'COLUMN_39, '); -- 납입방법

        -- 40-42
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_40, ', -- 종구분
            'COLUMN_41, ', -- 군구분
            'COLUMN_42, '); -- 계약상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_40, ', -- 종구분
            'COLUMN_41, ', -- 군구분
            'COLUMN_42, '); -- 계약상태

        -- 43-45
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_43, ', -- 차량구분
            'COLUMN_44, ', -- 사업/비사업
            'COLUMN_45, '); -- 차종
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_43, ', -- 차량구분
            'COLUMN_44, ', -- 사업/비사업
            'COLUMN_45, '); -- 차종

        -- 46-48
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_46, ', -- 차량번호
            'COLUMN_47, ', -- 전계약사
            'COLUMN_48, '); -- 납입기간
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_46, ', -- 차량번호
            'COLUMN_47, ', -- 전계약사
            'COLUMN_48, '); -- 납입기간

        -- 49-51
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_49, ', -- 물건
            'COLUMN_50, ', -- 공동인수여부
            'COLUMN_51, '); -- 사용인
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_49, ', -- 물건
            'COLUMN_50, ', -- 공동인수여부
            'COLUMN_51, '); -- 사용인

        -- 52-54
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_52, ', -- 사용인명
            'COLUMN_53, ', -- 미수여부
            'COLUMN_54, '); -- 장기신규(자동이체여부)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_52, ', -- 사용인명
            'COLUMN_53, ', -- 미수여부
            'COLUMN_54, '); -- 장기신규(자동이체여부)

        -- 55-57
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_55, ', -- 자동이체 일자
            'COLUMN_56, ', -- 동일증권유무
            'COLUMN_57, '); -- 보종구분값
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_55, ', -- 자동이체 일자
            'COLUMN_56, ', -- 동일증권유무
            'COLUMN_57, '); -- 보종구분값

        -- 58-60
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_58, ', -- 전자서명여부
            'COLUMN_59, ', -- 자기계약여부
            'COLUMN_60, '); -- 승환계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_58, ', -- 전자서명여부
            'COLUMN_59, ', -- 자기계약여부
            'COLUMN_60, '); -- 승환계약여부

        -- 61-63
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_61, ', -- 승환계약상태명
            'COLUMN_62, ', -- 자동차(신/타)
            'COLUMN_63, '); -- 일반실적그룹
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_61, ', -- 승환계약상태명
            'COLUMN_62, ', -- 자동차(신/타)
            'COLUMN_63, '); -- 일반실적그룹

        -- 64-66
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_64, ', -- 일반실적구분
            'COLUMN_65, ', -- 일반실적구분코드
            'COLUMN_66, '); -- 전자서명일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_64, ', -- 일반실적구분
            'COLUMN_65, ', -- 일반실적구분코드
            'COLUMN_66, '); -- 전자서명일자

        -- 67-69
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_67, ', -- 전자서명시간
            'COLUMN_68, ', -- 모대리점코드
            'COLUMN_69, '); -- 모대리점명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_67, ', -- 전자서명시간
            'COLUMN_68, ', -- 모대리점코드
            'COLUMN_69, '); -- 모대리점명

        -- 70-72
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_70, ', -- 모대리점 관리본부
            'COLUMN_71, ', -- 장기할증전보험료
            'COLUMN_72, '); -- 보장보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_70, ', -- 모대리점 관리본부
            'COLUMN_71, ', -- 장기할증전보험료
            'COLUMN_72, '); -- 보장보험료

        -- 73-75
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_73, ', -- 적립보험료
            'COLUMN_74, ', -- 설계번호
            'COLUMN_75, '); -- 피보험자명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_73, ', -- 적립보험료
            'COLUMN_74, ', -- 설계번호
            'COLUMN_75, '); -- 피보험자명

        -- 76-78
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_76, ', -- 물건명1
            'COLUMN_77, ', -- 물건명2
            'COLUMN_78, '); -- 물건명3
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_76, ', -- 물건명1
            'COLUMN_77, ', -- 물건명2
            'COLUMN_78, '); -- 물건명3

        -- 79-81
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_79, ', -- 플랜
            'COLUMN_80, ', -- 중개인
            'COLUMN_81, '); -- 중개인명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_79, ', -- 플랜
            'COLUMN_80, ', -- 중개인
            'COLUMN_81, '); -- 중개인명

        -- 82-84
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_82, ', -- 설계지원최초취급자코드
            'COLUMN_83, ', -- 설계지원최초취급자명
            'COLUMN_84, '); -- 설계지원최종취급자코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_82, ', -- 설계지원최초취급자코드
            'COLUMN_83, ', -- 설계지원최초취급자명
            'COLUMN_84, '); -- 설계지원최종취급자코드

        -- 85-87
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_85, ', -- 설계지원최종취급자명
            'COLUMN_86, ', -- 출생후계속보험료
            'COLUMN_87, '); -- 수입보험료입력일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_85, ', -- 설계지원최종취급자명
            'COLUMN_86, ', -- 출생후계속보험료
            'COLUMN_87, '); -- 수입보험료입력일자

        -- 88-90
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_88, ', -- 유의계약여부
            'COLUMN_89, ', -- 최초취급자코드(계약자주민번호기준)
            'COLUMN_90, '); -- 최조취급자명(계약자주민번호기준)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_88, ', -- 유의계약여부
            'COLUMN_89, ', -- 최초취급자코드(계약자주민번호기준)
            'COLUMN_90, '); -- 최조취급자명(계약자주민번호기준)

        -- 91-93
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_91, ', -- 할인전 보험료
            'COLUMN_92, ', -- 마케팅활용동의여부
            'COLUMN_93, '); -- 계약자고객ID
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_91, ', -- 할인전 보험료
            'COLUMN_92, ', -- 마케팅활용동의여부
            'COLUMN_93, '); -- 계약자고객ID

        -- 94-96
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_94, ', -- 일반무해지구분
            'COLUMN_95, ', -- 청약일시
            'COLUMN_96, '); -- 실손전환전갱신주기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_94, ', -- 일반무해지구분
            'COLUMN_95, ', -- 청약일시
            'COLUMN_96, '); -- 실손전환전갱신주기

        -- 97-99
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_97, ', -- 취소일자
            'COLUMN_98, ', -- 지급사유코드명
            'COLUMN_99, '); -- 크로스계약여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_97, ', -- 취소일자
            'COLUMN_98, ', -- 지급사유코드명
            'COLUMN_99, '); -- 크로스계약여부

        -- 100-102
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_100, ', -- 당사승환회차
            'COLUMN_101, ', -- 피보험자수
            'COLUMN_102, '); -- 보장보험료구성비
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_100, ', -- 당사승환회차
            'COLUMN_101, ', -- 피보험자수
            'COLUMN_102, '); -- 보장보험료구성비

        -- 103-105
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_103, ', -- 팀명
            'COLUMN_104, ', -- w설계시작여부
            'COLUMN_105, '); -- w설계활용여부
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_103, ', -- 팀명
            'COLUMN_104, ', -- w설계시작여부
            'COLUMN_105, '); -- w설계활용여부

        -- 106-108
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_106, ', -- EtoE여부
            'COLUMN_107, ', -- 차량등급코드
            'COLUMN_108, '); -- 권유직원
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_106, ', -- EtoE여부
            'COLUMN_107, ', -- 차량등급코드
            'COLUMN_108, '); -- 권유직원

        -- 109-110
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_109, ', -- 피보험자고객ID
            'NULL');        -- 납기구분
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_109, ', -- 피보험자고객ID
            'COLUMN_110');  -- 납기구분
    ELSEIF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
        SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
        SET v_processed_table = 'T_RPA_LONG_TERM_PROCESSED';

        -- Mapping for EXT LTR contracts (Columns 01-34)
        SET v_raw_cols = ''; SET v_proc_cols = '';

        -- 01-03
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_01, ', -- 증권번호
            'COLUMN_02, ', -- 계약자명
            'COLUMN_03, '); -- 납입주기
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_01, ', -- 증권번호
            'COLUMN_02, ', -- 계약자명
            'COLUMN_03, '); -- 납입주기

        -- 04-06
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_04, ', -- 납입회차
            'COLUMN_05, ', -- 납입년월
            'COLUMN_06, '); -- 납입일자
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_04, ', -- 납입회차
            'COLUMN_05, ', -- 납입년월
            'COLUMN_06, '); -- 납입일자

        -- 07-09
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_07, ', -- 납입기간
            'COLUMN_08, ', -- 초회보험료
            'COLUMN_09, '); -- 적용보험료
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_07, ', -- 납입기간
            'COLUMN_08, ', -- 초회보험료
            'COLUMN_09, '); -- 적용보험료

        -- 10-12
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_10, ', -- 상태코드
            'COLUMN_11, ', -- 상태
            'COLUMN_12, '); -- 실적기준일(변경일자)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_10, ', -- 상태코드
            'COLUMN_11, ', -- 상태
            'COLUMN_12, '); -- 실적기준일(변경일자)

        -- 13-15
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_13, ', -- 보험시기
            'COLUMN_14, ', -- 보험종기
            'COLUMN_15, '); -- 수금방법
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_13, ', -- 보험시기
            'COLUMN_14, ', -- 보험종기
            'COLUMN_15, '); -- 수금방법

        -- 16-18
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_16, ', -- 부지역단(최종)
            'COLUMN_17, ', -- 취급점포(최종)
            'COLUMN_18, '); -- 취급자(최종)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_16, ', -- 부지역단(최종)
            'COLUMN_17, ', -- 취급점포(최종)
            'COLUMN_18, '); -- 취급자(최종)

        -- 19-21
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_19, ', -- 사용인(최종)
            'COLUMN_20, ', -- 사용인코드(최종)
            'COLUMN_21, '); -- 모집조직(최초)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_19, ', -- 사용인(최종)
            'COLUMN_20, ', -- 사용인코드(최종)
            'COLUMN_21, '); -- 모집조직(최초)

        -- 22-24
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_22, ', -- 부지역단(최초)
            'COLUMN_23, ', -- 취급점포(최초)
            'COLUMN_24, '); -- 사용인(최초)
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_22, ', -- 부지역단(최초)
            'COLUMN_23, ', -- 취급점포(최초)
            'COLUMN_24, '); -- 사용인(최초)

        -- 25-27
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_25, ', -- 사용인코드(최초)
            'COLUMN_26, ', -- 보종코드
            'COLUMN_27, '); -- 보종명
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_25, ', -- 사용인코드(최초)
            'COLUMN_26, ', -- 보종코드
            'COLUMN_27, '); -- 보종명

        -- 28-30
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_28, ', -- 계약자생년월일
            'COLUMN_29, ', -- 피보험자명
            'COLUMN_30, '); -- 피보험자생년월일
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_28, ', -- 계약자생년월일
            'COLUMN_29, ', -- 피보험자명
            'COLUMN_30, '); -- 피보험자생년월일

        -- 31-33
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_31, ', -- 수정환산보험료
            'COLUMN_32, ', -- 이체일자
            'COLUMN_33, '); -- 세부상태코드
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_31, ', -- 수정환산보험료
            'COLUMN_32, ', -- 이체일자
            'COLUMN_33, '); -- 세부상태코드

        -- 34
        SET v_raw_cols = CONCAT(v_raw_cols,
            'COLUMN_34'); -- 세부상태
        SET v_proc_cols = CONCAT(v_proc_cols,
            'COLUMN_34'); -- 세부상태

    END IF;
    
    -- 2. Execute if column mapping is valid
    IF v_raw_cols != '' AND v_proc_cols != '' THEN

        -- 2.1. Create temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_LTG_PROCESSED LIKE ', v_processed_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- 2.2. Insert raw data into temp table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_LTG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''', v_company_code, ''', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''', v_company_code, ''' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''');'
        );
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- [DEBUG] Capture initial counts
        SET @sql_raw_count = CONCAT(
            'SELECT COUNT(*) INTO @v_raw_count FROM ', v_raw_table,
            ' WHERE BATCH_ID = ''', IN_BATCH_ID, ''' ',
            'AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            'AND COMPANY_CODE = ''LTG'''
        );
        PREPARE stmt_raw FROM @sql_raw_count;
        EXECUTE stmt_raw;
        DEALLOCATE PREPARE stmt_raw;
        SET v_log_initial_raw = @v_raw_count;

        SELECT COUNT(*) INTO v_log_temp_initial FROM T_TEMP_RPA_LTG_PROCESSED;
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'INITIAL_RAW', v_log_initial_raw, NOW());
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'TEMP_INITIAL', v_log_temp_initial, NOW());

        -- 2.3. Apply transformation rules (LTR / CAR / GEN)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* Rule 1: Column1열 값 변경(1개)
               항목명I : 납기구분 / 항목값 : 년납
               ※ 전체 행에 반영 */
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET COLUMN_110 = '년납';

            SELECT COUNT(*)
              INTO v_log_after_rule1
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_1', v_log_after_rule1, NOW()
            );

            /* Rule 2: [처리구분]≠"신규/추징"이면 데이터 행삭제 */
            DELETE
              FROM T_TEMP_RPA_LTG_PROCESSED
             WHERE COLUMN_23 != '신규/추징';

            SELECT COUNT(*)
              INTO v_log_after_rule2
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_2', v_log_after_rule2, NOW()
            );

            /* Rule 3.1: [증권번호] 오름차순 정렬 */
            SET @seq := 0;
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET SORT_ORDER_NO = (@seq := @seq + 1)
             ORDER BY COLUMN_08 ASC, EXCEL_ROW_INDEX ASC;

            /* Rule 3.2: 중복 증권번호 중 [처리구분]=각각 "정상,취소/철회"
               이면 [처리구분]="취소" 값수정 및 [보험료]="마이너스금액" 데이터 행삭제 */
            DROP TEMPORARY TABLE IF EXISTS tmp_ltg_dup_case;
            CREATE TEMPORARY TABLE tmp_ltg_dup_case
            SELECT COLUMN_08
              FROM T_TEMP_RPA_LTG_PROCESSED
             GROUP BY COLUMN_08
            HAVING SUM(CASE WHEN COLUMN_23 = '정상' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_23 = '취소/철회' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_LTG_PROCESSED t
            INNER JOIN tmp_ltg_dup_case d
                    ON t.COLUMN_08 = d.COLUMN_08
               SET t.COLUMN_23 = '취소';

            DELETE
              FROM T_TEMP_RPA_LTG_PROCESSED
             WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_ltg_dup_case)
               AND REPLACE(IFNULL(COLUMN_30, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*)
              INTO v_log_after_rule3
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_3', v_log_after_rule3, NOW()
            );

            /* Rule 4: 납입기간="100"면 원수사 원부확인하여 "년납"으로 값수정 */
            /* [PAUSE/SKIP] 원수사 원부확인 수동 처리 필요 */

        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* Rule 1: Column1열 값 변경(1개)
               항목명I : 납기구분 / 항목값 : 년납
               ※ 전체 행에 반영 */
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET COLUMN_110 = '년납';

            SELECT COUNT(*)
              INTO v_log_after_rule1
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_1', v_log_after_rule1, NOW()
            );

            /* Rule 2: [처리구분]≠"신규/추징"이면 데이터 행삭제 */
            DELETE
              FROM T_TEMP_RPA_LTG_PROCESSED
             WHERE COLUMN_23 != '신규/추징';

            SELECT COUNT(*)
              INTO v_log_after_rule2
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_2', v_log_after_rule2, NOW()
            );

            /* Rule 3.1: [증권번호] 오름차순 정렬 */
            SET @seq := 0;
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET SORT_ORDER_NO = (@seq := @seq + 1)
             ORDER BY COLUMN_08 ASC, EXCEL_ROW_INDEX ASC;

            /* Rule 3.2: 중복 증권번호 중 [처리구분]=각각 "정상,취소/철회"
               이면 [처리구분]="취소" 값수정 및 [보험료]="마이너스금액" 데이터 행삭제 */
            DROP TEMPORARY TABLE IF EXISTS tmp_ltg_dup_case;
            CREATE TEMPORARY TABLE tmp_ltg_dup_case
            SELECT COLUMN_08
              FROM T_TEMP_RPA_LTG_PROCESSED
             GROUP BY COLUMN_08
            HAVING SUM(CASE WHEN COLUMN_23 = '정상' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_23 = '취소/철회' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_LTG_PROCESSED t
            INNER JOIN tmp_ltg_dup_case d
                    ON t.COLUMN_08 = d.COLUMN_08
               SET t.COLUMN_23 = '취소';

            DELETE
              FROM T_TEMP_RPA_LTG_PROCESSED
             WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_ltg_dup_case)
               AND REPLACE(IFNULL(COLUMN_30, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*)
              INTO v_log_after_rule3
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_3', v_log_after_rule3, NOW()
            );
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' AND UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            /* Rule 1: 맨 마지막열 값 추가(1개)
               항목명I : 납기구분 / 항목값 : 년납
               ※ 전체 행에 반영 */
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET COLUMN_110 = '년납';

            SELECT COUNT(*)
              INTO v_log_after_rule1
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_1', v_log_after_rule1, NOW()
            );

            /* Rule 2: [처리구분]≠"신규/추징"이면 데이터 행삭제 */
            DELETE
              FROM T_TEMP_RPA_LTG_PROCESSED
             WHERE COLUMN_23 != '신규/추징';

            SELECT COUNT(*)
              INTO v_log_after_rule2
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_2', v_log_after_rule2, NOW()
            );

            /* Rule 3.1: [증권번호] 오름차순 정렬 */
            SET @seq := 0;
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET SORT_ORDER_NO = (@seq := @seq + 1)
             ORDER BY COLUMN_08 ASC, EXCEL_ROW_INDEX ASC;

            /* Rule 3.2: 중복 증권번호 중 [처리구분]=각각 "정상,취소/철회"
               이면 [처리구분]="취소" 값수정 및 [보험료]="마이너스금액" 데이터 행삭제 */
            DROP TEMPORARY TABLE IF EXISTS tmp_ltg_dup_case;
            CREATE TEMPORARY TABLE tmp_ltg_dup_case
            SELECT COLUMN_08
              FROM T_TEMP_RPA_LTG_PROCESSED
             GROUP BY COLUMN_08
            HAVING SUM(CASE WHEN COLUMN_23 = '정상' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN COLUMN_23 = '취소/철회' THEN 1 ELSE 0 END) > 0;

            UPDATE T_TEMP_RPA_LTG_PROCESSED t
            INNER JOIN tmp_ltg_dup_case d
                    ON t.COLUMN_08 = d.COLUMN_08
               SET t.COLUMN_23 = '취소';

            DELETE
              FROM T_TEMP_RPA_LTG_PROCESSED
             WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_ltg_dup_case)
               AND REPLACE(IFNULL(COLUMN_30, '0'), ',', '') REGEXP '^-[0-9]+';

            SELECT COUNT(*)
              INTO v_log_after_rule3
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_3', v_log_after_rule3, NOW()
            );
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'LTR' AND UPPER(IN_CONTRACT_TYPE) = 'EXT' THEN
             /* Rule 1: [상태]=“정상,불능”이면 [실적기준일(변경일자)]=“0000-00-00”으로 수정 */
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET COLUMN_12 = '0000-00-00'
             WHERE COLUMN_11 IN ('정상', '불능');

            SELECT COUNT(*)
              INTO v_log_after_rule1
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_1', v_log_after_rule1, NOW()
            );

            /* Rule 2: [상태]=“정상” & [세부상태항목]≠"납입면제,"완납"이면
               [납입년월] 연체건은 [상태]값을 "연체"로 수정
               연체기준 : 최종납입월도가 마감월도보다 작은 경우 */
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET COLUMN_11 = '연체'
             WHERE COLUMN_11 = '정상'
               AND COLUMN_34 NOT LIKE '%납입면제%'
               AND COLUMN_34 NOT LIKE '%완납%'
               AND COLUMN_05 < v_current_ym;

            SELECT COUNT(*)
              INTO v_log_after_rule2
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_2', v_log_after_rule2, NOW()
            );

            /* Rule 3: [상태]=“실효” & [납입년월]=“실효 3년 경과”면,
               [상태]값을 “시효”로 변경
               3년 경과 기준 : 마감월도 2025.12월 기준 최종납입월이 2022.10월 이하 */
            UPDATE T_TEMP_RPA_LTG_PROCESSED
               SET COLUMN_11 = '시효'
             WHERE COLUMN_11 = '실효'
               AND COLUMN_05 <= v_cutoff_ym;

            SELECT COUNT(*)
              INTO v_log_after_rule3
              FROM T_TEMP_RPA_LTG_PROCESSED;
            INSERT INTO T_RPA_DEBUG_LOG VALUES (
                IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE,
                'AFTER_RULE_3', v_log_after_rule3, NOW()
            );
        END IF;

        -- 2.4. Insert transformed data into processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_processed_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_LTG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();
        INSERT INTO T_RPA_DEBUG_LOG VALUES (IN_BATCH_ID, 'LTG', IN_INSURANCE_TYPE, IN_CONTRACT_TYPE, 'FINAL_INSERT', v_row_count, NOW());

        -- 2.5. Drop temp table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;

    END IF;

END
