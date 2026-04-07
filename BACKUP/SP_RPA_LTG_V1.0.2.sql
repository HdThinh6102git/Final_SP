CREATE DEFINER=`root`@`localhost` PROCEDURE `rpa_insurance`.`SP_RPA_LTG`(
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
    DECLARE v_company_code VARCHAR(10)  DEFAULT 'LTG';
    DECLARE v_target_ym    VARCHAR(6)   DEFAULT '';
    DECLARE v_cutoff_ym    VARCHAR(6)   DEFAULT '';

    -- [DECLARE handler]
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
    END;

    -- [SET logic]
    SET v_target_ym = DATE_FORMAT(NOW(), '%Y%m');
    -- 시효 기준: 마감월 대비 38개월 이전
    SET v_cutoff_ym = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 38 MONTH), '%Y%m');

    -- 1. Hardcoded Column Mapping for Lotte Insurance (LTG)
    -- 1. Hardcoded Column Mapping for Lotte Insurance (LTG)
    -- 1. Hardcoded Column Mapping for Lotte Insurance (LTG)
    IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
        -- Mapping for NEW LTR/GEN (Columns 01-109 + Target-only 110)
        IF UPPER(IN_INSURANCE_TYPE) IN ('LTR', 'GEN') THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 부문코드
                'COLUMN_02, ', -- 지역단코드
                'COLUMN_03, '); -- 지역단명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 부문코드
                'COLUMN_02, ', -- 지역단코드
                'COLUMN_03, '); -- 부문코드, 지역단코드, 지역단명
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 지점코드
                'COLUMN_05, ', -- 지점명
                'COLUMN_06, '); -- 취급자코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 지점코드
                'COLUMN_05, ', -- 지점명
                'COLUMN_06, '); -- 지점코드, 지점명, 취급자코드
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 취급자명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 출장소(지사코드)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 취급자명
                'COLUMN_08, ', -- 증권번호
                'COLUMN_09, '); -- 취급자명, 증권번호, 출장소(지사코드)
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 팀코드(지사명)
                'COLUMN_11, ', -- 관리사원
                'COLUMN_12, '); -- 모집자코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 팀코드(지사명)
                'COLUMN_11, ', -- 관리사원
                'COLUMN_12, '); -- 팀코드(지사명), 관리사원, 모집자코드
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 모집자명
                'COLUMN_14, ', -- 보험구분
                'COLUMN_15, '); -- 보종코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 모집자명
                'COLUMN_14, ', -- 보험구분
                'COLUMN_15, '); -- 모집자명, 보험구분, 보종코드
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 보종명
                'COLUMN_17, ', -- 실적일자
                'COLUMN_18, '); -- 보험시기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 보종명
                'COLUMN_17, ', -- 실적일자
                'COLUMN_18, '); -- 보종명, 실적일자, 보험시기
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 보험종기
                'COLUMN_20, ', -- 수납실적일자
                'COLUMN_21, '); -- 주민번호
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 보험종기
                'COLUMN_20, ', -- 수납실적일자
                'COLUMN_21, '); -- 보험종기, 수납실적일자, 주민번호
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 보험계약자명
                'COLUMN_23, ', -- 처리구분
                'COLUMN_24, '); -- 실적건수
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 보험계약자명
                'COLUMN_23, ', -- 처리구분
                'COLUMN_24, '); -- 보험계약자명, 처리구분, 실적건수
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 가계구분
                'COLUMN_26, ', -- 수금방법
                'COLUMN_27, '); -- 금종구분
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 가계구분
                'COLUMN_26, ', -- 수금방법
                'COLUMN_27, '); -- 가계구분, 수금방법, 금종구분
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 수당응당월
                'COLUMN_29, ', -- 납입회차
                'COLUMN_30, '); -- 당사보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 수당응당월
                'COLUMN_29, ', -- 납입회차
                'COLUMN_30, '); -- 수당응당월, 납입회차, 당사보험료
            
            -- 31-33
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_31, ', -- 신계약수정P
                'COLUMN_32, ', -- 수금수정P
                'COLUMN_33, '); -- 수당합계
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 신계약수정P
                'COLUMN_32, ', -- 수금수정P
                'COLUMN_33, '); -- 신계약수정P, 수금수정P, 수당합계
            
            -- 34-36
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_34, ', -- 원수실적
                'COLUMN_35, ', -- 수입실적
                'COLUMN_36, '); -- 수납실적
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_34, ', -- 원수실적
                'COLUMN_35, ', -- 수입실적
                'COLUMN_36, '); -- 원수실적, 수입실적, 수납실적
            
            -- 37-39
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_37, ', -- 수당지급여부
                'COLUMN_38, ', -- 입금상태
                'COLUMN_39, '); -- 납입방법
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_37, ', -- 수당지급여부
                'COLUMN_38, ', -- 입금상태
                'COLUMN_39, '); -- 수당지급여부, 입금상태, 납입방법
            
            -- 40-42
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_40, ', -- 종구분
                'COLUMN_41, ', -- 군구분
                'COLUMN_42, '); -- 계약상태
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_40, ', -- 종구분
                'COLUMN_41, ', -- 군구분
                'COLUMN_42, '); -- 종구분, 군구분, 계약상태
            
            -- 43-45
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_43, ', -- 차량구분
                'COLUMN_44, ', -- 사업/비사업
                'COLUMN_45, '); -- 차종
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_43, ', -- 차량구분
                'COLUMN_44, ', -- 사업/비사업
                'COLUMN_45, '); -- 차량구분, 사업/비사업, 차종
            
            -- 46-48
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_46, ', -- 차량번호
                'COLUMN_47, ', -- 전계약사
                'COLUMN_48, '); -- 납입기간
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_46, ', -- 차량번호
                'COLUMN_47, ', -- 전계약사
                'COLUMN_48, '); -- 차량번호, 전계약사, 납입기간
            
            -- 49-51
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_49, ', -- 물건
                'COLUMN_50, ', -- 공동인수여부
                'COLUMN_51, '); -- 사용인
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_49, ', -- 물건
                'COLUMN_50, ', -- 공동인수여부
                'COLUMN_51, '); -- 물건, 공동인수여부, 사용인
            
            -- 52-54
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_52, ', -- 사용인명
                'COLUMN_53, ', -- 미수여부
                'COLUMN_54, '); -- 장기신규(용)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_52, ', -- 사용인명
                'COLUMN_53, ', -- 미수여부
                'COLUMN_54, '); -- 사용인명, 미수여부, 장기신규(용)
            
            -- 55-57
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_55, ', -- 자동이체일자
                'COLUMN_56, ', -- 동일증권유무
                'COLUMN_57, '); -- 보종구분값
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_55, ', -- 자동이체일자
                'COLUMN_56, ', -- 동일증권유무
                'COLUMN_57, '); -- 자동이체일자, 동일증권유무, 보종구분값
            
            -- 58-60
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_58, ', -- 전자서명여부
                'COLUMN_59, ', -- 자기계약여부
                'COLUMN_60, '); -- 승환계약여부
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_58, ', -- 전자서명여부
                'COLUMN_59, ', -- 자기계약여부
                'COLUMN_60, '); -- 전자서명여부, 자기계약여부, 승환계약여부
            
            -- 61-63
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_61, ', -- 승환계약상태
                'COLUMN_62, ', -- 자동차(신/타)
                'COLUMN_63, '); -- 일반실적그룹
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_61, ', -- 승환계약상태
                'COLUMN_62, ', -- 자동차(신/타)
                'COLUMN_63, '); -- 승환계약상태, 자동차(신/타), 일반실적그룹
            
            -- 64-66
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_64, ', -- 일반실적구분
                'COLUMN_65, ', -- 일반실적구분코드
                'COLUMN_66, '); -- 전자서명일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_64, ', -- 일반실적구분
                'COLUMN_65, ', -- 일반실적구분코드
                'COLUMN_66, '); -- 일반실적구분, 일반실적구분코드, 전자서명일자
            
            -- 67-69
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_67, ', -- 전자서명시간
                'COLUMN_68, ', -- 모대리점코드
                'COLUMN_69, '); -- 모대리점명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_67, ', -- 전자서명시간
                'COLUMN_68, ', -- 모대리점코드
                'COLUMN_69, '); -- 전자서명시간, 모대리점코드, 모대리점명
            
            -- 70-72
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_70, ', -- 모대리점관리본부
                'COLUMN_71, ', -- 장기할증전P
                'COLUMN_72, '); -- 보장보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_70, ', -- 모대리점관리본부
                'COLUMN_71, ', -- 장기할증전P
                'COLUMN_72, '); -- 모대리점관리본부, 장기할증전P, 보장보험료
            
            -- 73-75
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_73, ', -- 적립보험료
                'COLUMN_74, ', -- 설계번호
                'COLUMN_75, '); -- 피보험자명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_73, ', -- 적립보험료
                'COLUMN_74, ', -- 설계번호
                'COLUMN_75, '); -- 적립보험료, 설계번호, 피보험자명
            
            -- 76-78
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_76, ', -- 물건명1
                'COLUMN_77, ', -- 물건명2
                'COLUMN_78, '); -- 물건명3
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_76, ', -- 물건명1
                'COLUMN_77, ', -- 물건명2
                'COLUMN_78, '); -- 물건명1, 물건명2, 물건명3
            
            -- 79-81
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_79, ', -- 플랜
                'COLUMN_80, ', -- 중개인
                'COLUMN_81, '); -- 중개인명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_79, ', -- 플랜
                'COLUMN_80, ', -- 중개인
                'COLUMN_81, '); -- 플랜, 중개인, 중개인명
            
            -- 82-84
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_82, ', -- 설계최초코드
                'COLUMN_83, ', -- 설계최초명
                'COLUMN_84, '); -- 설계최종코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_82, ', -- 설계최초코드
                'COLUMN_83, ', -- 설계최초명
                'COLUMN_84, '); -- 설계최초코드, 설계최초명, 설계최종코드
            
            -- 85-87
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_85, ', -- 설계최종명
                'COLUMN_86, ', -- 출생후계속P
                'COLUMN_87, '); -- 수입보험료일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_85, ', -- 설계최종명
                'COLUMN_86, ', -- 출생후계속P
                'COLUMN_87, '); -- 설계최종명, 출생후계속P, 수입보험료일자
            
            -- 88-90
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_88, ', -- 유의계약여부
                'COLUMN_89, ', -- 최초취급코드
                'COLUMN_90, '); -- 최초취급명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_88, ', -- 유의계약여부
                'COLUMN_89, ', -- 최초취급코드
                'COLUMN_90, '); -- 유의계약여부, 최초취급코드, 최초취급명
            
            -- 91-93
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_91, ', -- 할인전P
                'COLUMN_92, ', -- 마케팅동의여부
                'COLUMN_93, '); -- 계약자고객ID
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_91, ', -- 할인전P
                'COLUMN_92, ', -- 마케팅동의여부
                'COLUMN_93, '); -- 할인전P, 마케팅동의여부, 계약자고객ID
            
            -- 94-96
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_94, ', -- 일반무해지구분
                'COLUMN_95, ', -- 청약일시
                'COLUMN_96, '); -- 실손갱신주기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_94, ', -- 일반무해지구분
                'COLUMN_95, ', -- 청약일시
                'COLUMN_96, '); -- 일반무해지구분, 청약일시, 실손갱신주기
            
            -- 97-99
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_97, ', -- 취소일자
                'COLUMN_98, ', -- 지급사유코드
                'COLUMN_99, '); -- 크로스계약
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_97, ', -- 취소일자
                'COLUMN_98, ', -- 지급사유코드
                'COLUMN_99, '); -- 취소일자, 지급사유코드, 크로스계약
            
            -- 100-102
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_100, ', -- 당사승환회차
                'COLUMN_101, ', -- 피보험자수
                'COLUMN_102, '); -- 보장P구성비
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_100, ', -- 당사승환회차
                'COLUMN_101, ', -- 피보험자수
                'COLUMN_102, '); -- 당사승환회차, 피보험자수, 보장P구성비
            
            -- 103-105
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_103, ', -- 팀명
                'COLUMN_104, ', -- w설계시작
                'COLUMN_105, '); -- w설계활용
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_103, ', -- 팀명
                'COLUMN_104, ', -- w설계시작
                'COLUMN_105, '); -- 팀명, w설계시작, w설계활용
            
            -- 106-108
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_106, ', -- EtoE여부
                'COLUMN_107, ', -- 차량등급코드
                'COLUMN_108, '); -- 권유직원
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_106, ', -- EtoE여부
                'COLUMN_107, ', -- 차량등급코드
                'COLUMN_108, '); -- EtoE여부, 차량등급코드, 권유직원
            
            -- 109-110
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_109, ', -- 피보험자고객ID
                'NULL'); -- 납기구분(Target)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_109, ', -- 피보험자고객ID
                'COLUMN_110'); -- 피보험자고객ID, 납기구분(Target)

        -- CAR is skipped
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
        END IF;

    ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') THEN
        -- Mapping for EXT (Columns 01-34)
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_cols = ''; SET v_proc_cols = '';
            
            -- 01-03
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_01, ', -- 증권번호
                'COLUMN_02, ', -- 계약자명
                'COLUMN_03, '); -- 납입주기
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_01, ', -- 증권번호
                'COLUMN_02, ', -- 계약자명
                'COLUMN_03, '); -- 증권번호, 계약자명, 납입주기
            
            -- 04-06
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_04, ', -- 납입회차
                'COLUMN_05, ', -- 납입년월
                'COLUMN_06, '); -- 납입일자
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_04, ', -- 납입회차
                'COLUMN_05, ', -- 납입년월
                'COLUMN_06, '); -- 납입회차, 납입년월, 납입일자
            
            -- 07-09
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_07, ', -- 납입기간
                'COLUMN_08, ', -- 초회보험료
                'COLUMN_09, '); -- 적용보험료
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_07, ', -- 납입기간
                'COLUMN_08, ', -- 초회보험료
                'COLUMN_09, '); -- 납입기간, 초회보험료, 적용보험료
            
            -- 10-12
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_10, ', -- 상태코드
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 실적기준일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_10, ', -- 상태코드
                'COLUMN_11, ', -- 상태
                'COLUMN_12, '); -- 상태코드, 상태, 실적기준일
            
            -- 13-15
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_13, ', -- 보험시기
                'COLUMN_14, ', -- 보험종기
                'COLUMN_15, '); -- 수금방법
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_13, ', -- 보험시기
                'COLUMN_14, ', -- 보험종기
                'COLUMN_15, '); -- 보험시기, 보험종기, 수금방법
            
            -- 16-18
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_16, ', -- 부지역단(최종)
                'COLUMN_17, ', -- 취급점포(최종)
                'COLUMN_18, '); -- 취급자(최종)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_16, ', -- 부지역단(최종)
                'COLUMN_17, ', -- 취급점포(최종)
                'COLUMN_18, '); -- 부지역단(최종), 취급점포(최종), 취급자(최종)
            
            -- 19-21
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_19, ', -- 사용인(최종)
                'COLUMN_20, ', -- 사용인코드(최종)
                'COLUMN_21, '); -- 모집조직(최초)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_19, ', -- 사용인(최종)
                'COLUMN_20, ', -- 사용인코드(최종)
                'COLUMN_21, '); -- 사용인(최종), 사용인코드(최종), 모집조직(최초)
            
            -- 22-24
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_22, ', -- 부지역단(최초)
                'COLUMN_23, ', -- 취급점포(최초)
                'COLUMN_24, '); -- 사용인(최초)
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_22, ', -- 부지역단(최초)
                'COLUMN_23, ', -- 취급점포(최초)
                'COLUMN_24, '); -- 부지역단(최초), 취급점포(최초), 사용인(최초)
            
            -- 25-27
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_25, ', -- 사용인코드(최초)
                'COLUMN_26, ', -- 보종코드
                'COLUMN_27, '); -- 보종명
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_25, ', -- 사용인코드(최초)
                'COLUMN_26, ', -- 보종코드
                'COLUMN_27, '); -- 사용인코드(최초), 보종코드, 보종명
            
            -- 28-30
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_28, ', -- 계약자생년월일
                'COLUMN_29, ', -- 피보험자명
                'COLUMN_30, '); -- 피보험자생년월일
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_28, ', -- 계약자생년월일
                'COLUMN_29, ', -- 피보험자명
                'COLUMN_30, '); -- 계약자생년월일, 피보험자명, 피보험자생년월일
            
            -- 31-33
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_31, ', -- 수정환산보험료
                'COLUMN_32, ', -- 이체일자
                'COLUMN_33, '); -- 세부상태코드
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_31, ', -- 수정환산보험료
                'COLUMN_32, ', -- 이체일자
                'COLUMN_33, '); -- 수정환산보험료, 이체일자, 세부상태코드
            
            -- 34
            SET v_raw_cols = CONCAT(v_raw_cols, 
                'COLUMN_34'); -- 세부상태
            SET v_proc_cols = CONCAT(v_proc_cols, 
                'COLUMN_34'); -- 세부상태
        END IF;
    END IF;


    -- 2. Build sql query insert temp table
    IF v_raw_cols != '' AND v_proc_cols != '' THEN
        
        -- Select Tables
        IF UPPER(IN_INSURANCE_TYPE) = 'LTR' THEN
            SET v_raw_table = 'T_RPA_LONG_TERM_RAW';
            SET v_proc_table = 'T_RPA_LONG_TERM_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'GEN' THEN
            SET v_raw_table = 'T_RPA_GENERAL_RAW';
            SET v_proc_table = 'T_RPA_GENERAL_PROCESSED';
        ELSEIF UPPER(IN_INSURANCE_TYPE) = 'CAR' THEN
            SET v_raw_table = 'T_RPA_CAR_RAW';
            SET v_proc_table = 'T_RPA_CAR_PROCESSED';
        END IF;

        -- Create Temporary Table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        SET @sql_create = CONCAT('CREATE TEMPORARY TABLE T_TEMP_RPA_LTG_PROCESSED LIKE ', v_proc_table);
        PREPARE stmt_create FROM @sql_create;
        EXECUTE stmt_create;
        DEALLOCATE PREPARE stmt_create;

        -- Insert into Temporary Table
        SET @sql_query = CONCAT(
            'INSERT INTO T_TEMP_RPA_LTG_PROCESSED (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT REPLACE(UUID(), ''-'', ''''), UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), ''LTG'', BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, EXCEL_ROW_INDEX, ', v_raw_cols, ' ',
            'FROM ', v_raw_table, ' ',
            'WHERE COMPANY_CODE = ''LTG'' ',
            '  AND BATCH_ID = ''', IN_BATCH_ID, ''' ',
            '  AND UPPER(CONTRACT_TYPE) = UPPER(''', IN_CONTRACT_TYPE, ''') ',
            '  AND COLUMN_08 <> ''증권번호'' AND COLUMN_01 <> ''증권번호'';'
        );
        
        PREPARE stmt FROM @sql_query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- 3. Apply Transformation Logic
        
        IF UPPER(IN_CONTRACT_TYPE) = 'NEW' THEN
            -- Rule 1: 납기구분 = 년납
            UPDATE T_TEMP_RPA_LTG_PROCESSED SET COLUMN_110 = '년납';

            -- Rule 2: [처리구분](23) ≠ "신규, 추징" 삭제
            DELETE FROM T_TEMP_RPA_LTG_PROCESSED WHERE COLUMN_23 NOT IN ('신규', '추징', '신규/추징');

            -- Rule 3: 중복 증권번호 처리
            DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;
            CREATE TEMPORARY TABLE tmp_dup_case SELECT COLUMN_08 FROM T_TEMP_RPA_LTG_PROCESSED GROUP BY COLUMN_08 HAVING SUM(COLUMN_23 IN ('취소', '철회', '취소/철회')) > 0 AND SUM(COLUMN_23='정상') > 0;
            UPDATE T_TEMP_RPA_LTG_PROCESSED t INNER JOIN tmp_dup_case d ON t.COLUMN_08 = d.COLUMN_08 SET t.COLUMN_23 = '취소' WHERE t.COLUMN_23 = '정상';
            DELETE FROM T_TEMP_RPA_LTG_PROCESSED WHERE COLUMN_08 IN (SELECT COLUMN_08 FROM tmp_dup_case) AND (COLUMN_30 LIKE '-%' OR CAST(REPLACE(COLUMN_30,',','') AS DECIMAL(18,0)) < 0);

        ELSEIF UPPER(IN_CONTRACT_TYPE) IN ('EXT', 'EXISTING') THEN
            -- Rule 1: [상태](11)="정상,불능" -> [실적기준일](12)="0000-00-00"
            UPDATE T_TEMP_RPA_LTG_PROCESSED SET COLUMN_12 = '0000-00-00' WHERE COLUMN_11 IN ('정상', '불능');

            -- Rule 2: 연체 처리
            UPDATE T_TEMP_RPA_LTG_PROCESSED
            SET COLUMN_11 = '연체'
            WHERE COLUMN_11 = '정상'
              AND COLUMN_34 NOT LIKE '%납입면제%' AND COLUMN_34 NOT LIKE '%완납%'
              AND COLUMN_05 < v_target_ym;

            -- Rule 3: 시효 처리
            UPDATE T_TEMP_RPA_LTG_PROCESSED SET COLUMN_11 = '시효' WHERE COLUMN_11 = '실효' AND COLUMN_05 <= v_cutoff_ym;
        END IF;

        -- 4. Build sql query insert processed table
        SET @sql_insert = CONCAT(
            'INSERT INTO ', v_proc_table, ' (SYS_ID, SYS_CREATE_DATE, SYS_MODIFY_DATE, CREATED_DT, COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ') ',
            'SELECT SYS_ID, UTC_TIMESTAMP(), UTC_TIMESTAMP(), UTC_TIMESTAMP(), COMPANY_CODE, BATCH_ID, CONTRACT_TYPE, EXCEL_ROW_INDEX, SORT_ORDER_NO, ', v_proc_cols, ' ',
            'FROM T_TEMP_RPA_LTG_PROCESSED ORDER BY SORT_ORDER_NO ASC;'
        );
        PREPARE stmt_insert FROM @sql_insert;
        EXECUTE stmt_insert;
        DEALLOCATE PREPARE stmt_insert;

        SET v_row_count = ROW_COUNT();

        -- 5. Drop temporary table
        DROP TEMPORARY TABLE IF EXISTS T_TEMP_RPA_LTG_PROCESSED;
        DROP TEMPORARY TABLE IF EXISTS tmp_dup_case;

    END IF;

END